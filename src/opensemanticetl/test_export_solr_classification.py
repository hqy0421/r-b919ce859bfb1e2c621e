#!/usr/bin/python3
# -*- coding: utf-8 -*-

import unittest

import export_solr


# In memory replacement of the Solr realtime get and update endpoints, so the
# read-recompute-write logic and the optimistic concurrency can be tested
# without a running Solr.
class fake_solr(object):

    def __init__(self):
        self.documents = {}
        self.version = 0
        self.conflicts = 0
        self.posts = []

    def _new_version(self):
        self.version += 1
        return self.version

    def get(self, docid, fields):
        document = self.documents.get(docid)
        if document is None:
            return None

        result = {}
        for field in fields:
            if field in document:
                result[field] = document[field]
        return result

    def update(self, docid, data, check_version):
        if check_version and '_version_' in data:
            current = self.documents.get(docid, {}).get('_version_')
            if data['_version_'] == 0:
                # Solr semantics: 0 means "document must not exist"
                conflict = current is not None
            else:
                conflict = current is not None and current != data['_version_']
            if conflict:
                self.conflicts += 1
                raise export_solr.classification_concurrency_error(
                    'version conflict')

        document = dict(self.documents.get(docid, {}))
        document['id'] = docid

        for field, operation in data.items():
            if field in ('id', '_version_'):
                continue
            if not isinstance(operation, dict):
                continue
            if 'set' in operation:
                document[field] = operation['set']

        document['_version_'] = self._new_version()
        self.documents[docid] = document
        self.posts.append(data)


class fake_exporter(export_solr.export_solr):

    def __init__(self, solr, retries=3):
        export_solr.export_solr.__init__(self, config={
            'classification_retries': retries,
        })
        self.store = solr

    def get_data(self, docid, fields):
        return self.store.get(docid, fields)

    def post(self, data=None, docid=None, commit=None, check_version=False):
        update = dict(data)
        docid = update.pop('id')
        self.store.update(docid, update, check_version=True)


class Test_export_solr_classification(unittest.TestCase):

    def setUp(self):
        self.solr = fake_solr()
        self.exporter = fake_exporter(self.solr)

    def query_tags(self, docid):
        document = self.solr.documents.get(docid, {})
        return document.get(self.exporter.classification_query_field, [])

    #
    # canonical representation and version comparison
    #

    def test_canonical_tags(self):
        self.assertEqual(export_solr.canonical_tags(None), [])
        self.assertEqual(export_solr.canonical_tags('a'), ['a'])
        self.assertEqual(export_solr.canonical_tags(['b', 'a', 'b', '', None]),
                         ['a', 'b'])

    def test_compare_versions(self):
        self.assertLess(export_solr.compare_versions(2, 10), 0)
        self.assertLess(export_solr.compare_versions('2', '10'), 0)
        self.assertLess(export_solr.compare_versions('1.2.9', '1.2.10'), 0)
        self.assertLess(export_solr.compare_versions(
            '2024-01-01T10:00:00Z', '2024-01-01T11:00:00Z'), 0)
        self.assertEqual(export_solr.compare_versions(5, 5), 0)
        self.assertGreater(export_solr.compare_versions(7, 3), 0)

    #
    # union of sources
    #

    def test_union_of_sources(self):
        docid = 'doc1'
        self.exporter.set_classification(docid, 'crm', 1, ['a', 'b'])
        self.exporter.set_classification(docid, 'dms', 1, ['b', 'c'])

        self.assertEqual(self.query_tags(docid), ['a', 'b', 'c'])

    def test_duplicate_tag_removed(self):
        docid = 'doc1'
        self.exporter.set_classification(docid, 'crm', 1, ['a', 'a', 'b'])
        self.assertEqual(self.query_tags(docid), ['a', 'b'])

    #
    # versions
    #

    def test_old_version_skipped(self):
        docid = 'doc1'
        self.exporter.set_classification(docid, 'crm', 2, ['new'])
        result = self.exporter.set_classification(docid, 'crm', 1, ['old'])

        self.assertEqual(result['status'], 'skipped')
        self.assertEqual(self.query_tags(docid), ['new'])

    def test_same_version_same_content_is_replay(self):
        docid = 'doc1'
        first = self.exporter.set_classification(docid, 'crm', 1, ['a', 'b'])
        posts_before = len(self.solr.posts)

        second = self.exporter.set_classification(docid, 'crm', 1, ['b', 'a'])

        self.assertEqual(first['status'], 'updated')
        self.assertEqual(second['status'], 'skipped')
        self.assertEqual(len(self.solr.posts), posts_before)
        self.assertEqual(self.query_tags(docid), ['a', 'b'])

    def test_same_version_different_content_conflict(self):
        docid = 'doc1'
        self.exporter.set_classification(docid, 'crm', 1, ['a'])

        with self.assertRaises(export_solr.classification_conflict_error):
            self.exporter.set_classification(docid, 'crm', 1, ['b'])

        self.assertEqual(self.query_tags(docid), ['a'])

    def test_newer_version_replaces_source_tags(self):
        docid = 'doc1'
        self.exporter.set_classification(docid, 'crm', 1, ['a', 'b'])
        self.exporter.set_classification(docid, 'crm', 2, ['c'])

        self.assertEqual(self.query_tags(docid), ['c'])

    #
    # retraction
    #

    def test_retraction_keeps_tags_of_other_sources(self):
        docid = 'doc1'
        self.exporter.set_classification(docid, 'crm', 1, ['a', 'b'])
        self.exporter.set_classification(docid, 'dms', 1, ['b', 'c'])

        self.exporter.remove_classification(docid, 'crm', 2)

        # b is still from dms, a was only from crm
        self.assertEqual(self.query_tags(docid), ['b', 'c'])

    def test_retraction_with_old_version_skipped(self):
        docid = 'doc1'
        self.exporter.set_classification(docid, 'crm', 5, ['a'])

        result = self.exporter.remove_classification(docid, 'crm', 4)

        self.assertEqual(result['status'], 'skipped')
        self.assertEqual(self.query_tags(docid), ['a'])

    #
    # atomicity and partial update
    #

    def test_state_and_query_field_written_in_same_update(self):
        docid = 'doc1'
        self.exporter.set_classification(docid, 'crm', 1, ['a'])

        update = self.solr.posts[-1]
        self.assertIn(self.exporter.classification_state_field, update)
        self.assertIn(self.exporter.classification_query_field, update)

    def test_other_fields_preserved(self):
        docid = 'doc1'
        self.solr.documents[docid] = {
            'id': docid,
            'title_txt': 'A title',
            'content_txt': 'Some content',
            'email_ss': ['a@example.com'],
            '_version_': self.solr._new_version(),
        }

        self.exporter.set_classification(docid, 'crm', 1, ['a'])

        document = self.solr.documents[docid]
        self.assertEqual(document['title_txt'], 'A title')
        self.assertEqual(document['content_txt'], 'Some content')
        self.assertEqual(document['email_ss'], ['a@example.com'])

    def test_new_document_created_without_version_check(self):
        docid = 'newdoc'
        result = self.exporter.set_classification(docid, 'crm', 1, ['a'])

        self.assertEqual(result['status'], 'updated')
        self.assertEqual(self.query_tags(docid), ['a'])

    #
    # concurrency
    #

    def test_version_conflict_is_retried(self):
        docid = 'doc1'
        self.exporter.set_classification(docid, 'crm', 1, ['a'])

        # simulate a concurrent writer between read and write: the first
        # attempt of the exporter reads an outdated _version_
        original = self.exporter.get_data
        calls = {'count': 0}

        def racing_get_data(docid, fields):
            result = original(docid, fields)
            calls['count'] += 1
            if calls['count'] == 1:
                # concurrent change after this read
                self.solr.documents[docid]['_version_'] = self.solr._new_version()
                self.solr.documents[docid]['title_txt'] = 'concurrent'
            return result

        self.exporter.get_data = racing_get_data

        self.exporter.set_classification(docid, 'dms', 1, ['b'])

        self.assertEqual(self.solr.conflicts, 1)
        self.assertEqual(self.query_tags(docid), ['a', 'b'])
        self.assertEqual(self.solr.documents[docid]['title_txt'], 'concurrent')

    def test_concurrent_creation_is_detected_and_merged(self):
        docid = 'doc1'

        # another writer creates the document between our read (not found)
        # and write; the version check "must not exist" detects it, so the
        # next attempt reads and merges instead of overwriting
        original = self.exporter.get_data
        calls = {'count': 0}

        def racing_get_data(docid, fields):
            result = original(docid, fields)
            calls['count'] += 1
            if calls['count'] == 1:
                self.solr.update(
                    docid,
                    {'title_txt': {'set': 'created by other writer'}},
                    check_version=False)
            return result

        self.exporter.get_data = racing_get_data

        self.exporter.set_classification(docid, 'crm', 1, ['a'])

        self.assertEqual(self.solr.conflicts, 1)
        self.assertEqual(self.query_tags(docid), ['a'])
        self.assertEqual(
            self.solr.documents[docid]['title_txt'], 'created by other writer')

    def test_version_conflict_gives_up_after_limited_retries(self):
        docid = 'doc1'
        self.exporter.set_classification(docid, 'crm', 1, ['a'])

        original = self.exporter.get_data

        def always_racing_get_data(docid, fields):
            result = original(docid, fields)
            self.solr.documents[docid]['_version_'] = self.solr._new_version()
            return result

        self.exporter.get_data = always_racing_get_data

        with self.assertRaises(export_solr.classification_concurrency_error):
            self.exporter.set_classification(docid, 'dms', 1, ['b'])

        # retries are limited (initial attempt + configured retries)
        self.assertEqual(self.solr.conflicts, 4)

    #
    # process entry point
    #

    def test_process_classification_contribution(self):
        parameters = {
            'id': 'doc1',
            'classification': {'source': 'crm', 'version': 1, 'tags': ['a']}
        }

        parameters, data = self.exporter.process(parameters=parameters, data={})

        self.assertEqual(parameters['classification_result']['status'], 'updated')
        self.assertEqual(self.query_tags('doc1'), ['a'])

    def test_process_batch_contributions_atomic(self):
        parameters = {
            'id': 'doc1',
            'classification': [
                {'source': 'crm', 'version': 1, 'tags': ['a', 'b']},
                {'source': 'dms', 'version': 1, 'tags': ['b', 'c']},
            ]
        }

        self.exporter.process(parameters=parameters, data={})

        self.assertEqual(self.query_tags('doc1'), ['a', 'b', 'c'])
        # one update for both sources: no interleaving by other writers
        self.assertEqual(len(self.solr.posts), 1)

    def test_batch_conflict_writes_nothing(self):
        self.exporter.set_classification('doc1', 'crm', 1, ['a'])
        posts_before = len(self.solr.posts)

        with self.assertRaises(export_solr.classification_conflict_error):
            self.exporter.update_classifications('doc1', [
                {'source': 'dms', 'version': 1, 'tags': ['b']},
                {'source': 'crm', 'version': 1, 'tags': ['different']},
            ])

        self.assertEqual(len(self.solr.posts), posts_before)
        self.assertEqual(self.query_tags('doc1'), ['a'])


if __name__ == '__main__':
    unittest.main()

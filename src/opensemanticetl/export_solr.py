#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import json
import requests
import sys
import time

import decimal

import urllib.request
import urllib.parse
import urllib.error

# Export data to Solr


# Raised if one source sends different content for a version that was
# already imported for a document (same version and different content).
class classification_conflict_error(Exception):
    pass


# Raised if a concurrent update changed the document too often, so the
# limited count of read-recompute-write attempts was exhausted.
class classification_concurrency_error(Exception):
    pass


# Canonical representation of a tag list: strings, without empty values,
# de-duplicated and sorted, so content of a version can be compared and
# replayed idempotently.
def canonical_tags(tags):

    if tags is None:
        return []

    if isinstance(tags, str):
        tags = [tags]

    canonical = set()

    for tag in tags:
        if tag is None:
            continue

        tag = str(tag)

        if tag == '':
            continue

        canonical.add(tag)

    return sorted(canonical)


# Compare two versions. Plain integers (also as numeric string) are compared
# numerically, dotted numeric versions (i.e. 1.2.10) component-wise, everything
# else (for example ISO timestamps) as strings.
def compare_versions(left, right):

    left_kind, left_value = _version_key(left)
    right_kind, right_value = _version_key(right)

    if left_kind is not None and left_kind == right_kind:
        return (left_value > right_value) - (left_value < right_value)

    left_string = str(left)
    right_string = str(right)
    return (left_string > right_string) - (left_string < right_string)


def _version_key(version):

    if isinstance(version, bool):
        return None, None

    if isinstance(version, int):
        return 'number', decimal.Decimal(version)

    if isinstance(version, float):
        return 'number', decimal.Decimal(str(version))

    if not isinstance(version, str):
        return None, None

    version = version.strip()

    if version == '':
        return None, None

    if version.isdigit():
        return 'number', decimal.Decimal(version)

    parts = version.split('.')
    if len(parts) > 1 and all(part.isdigit() for part in parts):
        return 'dotted', tuple(int(part) for part in parts)

    return None, None


class export_solr(object):

    def __init__(self, config=None):
        if config is None:
            config = {}

        self.config = config

        if os.getenv('OPEN_SEMANTIC_ETL_SOLR'):
            self.config['solr'] = os.getenv('OPEN_SEMANTIC_ETL_SOLR')

        if not 'solr' in self.config:
            self.config['solr'] = 'http://localhost:8983/solr/'

        if not 'index' in self.config:
            self.config['index'] = 'opensemanticsearch'

        self.solr = self.config['solr']
        self.core = self.config['index']

        if not 'verbose' in self.config:
            self.config['verbose'] = False

        self.verbose = self.config['verbose']

        # fields and retries for optional multi source classification sync
        # query facing field: de-duplicated union of the tags of all sources
        self.classification_query_field = self.config.get(
            'classification_query_field', 'classification_tags_ss')

        # internal field: one JSON value per source with version and tags,
        # so a source can be retracted without loosing tags of other sources
        self.classification_state_field = self.config.get(
            'classification_state_field', 'classification_source_ss')

        # count of read-recompute-write attempts on concurrent updates
        self.classification_retries = self.config.get(
            'classification_retries', 5)

    #
    # Write data to Solr
    #

    def process(self, parameters=None, data=None):
        if parameters is None:
            parameters = {}
        if data is None:
            data = {}

        # if not there, set config defaults
        if 'verbose' in parameters:
            self.verbose = parameters['verbose']

        if self.verbose:            
            print ('Starting Exporter: Solr')

        if 'solr' in parameters:
            self.solr = parameters['solr']
            if not self.solr.endswith('/'):
                self.solr += '/'

        if 'index' in parameters:
            self.core = parameters['index']

        add = parameters.get('add', False)

        fields_set = parameters.get('fields_set', [])

        commit = parameters.get('commit', None)

        if not 'id' in data:
            data['id'] = parameters['id']

        #
        # Optional multi source classification sync: if the caller sends a
        # classification contribution (or a list of them), only the versioned
        # classification update is done instead of a regular export.
        #
        classification = parameters.get('classification')
        if classification:
            if isinstance(classification, dict):
                classification = [classification]

            result = self.update_classifications(
                docid=data['id'], contributions=classification, commit=commit)

            parameters['classification_result'] = result

            return parameters, data

        # post data to Solr
        do_export = True

        # but do not post, if only id (which will contain no add or set commands for fields and will be seen as overwrite for whole document)
        if len(data) < 2:
            if self.verbose:
                print('Not exported to Solr because no data or only the ID.')
            do_export = False

        # and do not post, if yet posted before (exporter not exporter, but in plugin queue, f.e. in multi stage processing before adding task to queue)
        if 'etl_export_solr_b' in data:
            if self.verbose:
                print('Not exported to Solr because no data or yet exported in this ETL run, because exporter was runned as plugin.')
                do_export = False

        if do_export:
            self.update(data=data, add=add, fields_set=fields_set, commit=commit)


        return parameters, data

    # update document in index, set fields in data to updated or new values or add new/additional values
    # if no document yet, it will be added
    def update(self, data, add=False, fields_set=(), commit=None):

        update_fields = {}

        for fieldname in data:
            if fieldname == 'id':
                update_fields['id'] = data['id']
            else:
                update_fields[fieldname] = {}

                if add and not fieldname in fields_set:
                    # add value to existent values of the field
                    update_fields[fieldname]['add-distinct'] = data[fieldname]
                else:
                    # if document there with values for this fields, the existing values will be overwritten with new values
                    update_fields[fieldname]['set'] = data[fieldname]

        self.post(data=update_fields, commit=commit)

    def post(self, data=None, docid=None, commit=None, check_version=False):
        if data is None:
            data = {}

        solr_uri = self.solr + self.core + '/update'

        if docid:
            data['id'] = docid

        datajson = '[' + json.dumps(data) + ']'

        params = {}

        if commit:
            params['commit'] = 'true'

        if self.verbose:
            print("Sending update request to {}".format(solr_uri))
            print(datajson)


        retries = 0
        retrytime = 1
        # wait time until next retry will be doubled until reaching maximum of 120 seconds (2 minutes) until next retry
        retrytime_max = 120
        no_connection = True

        while no_connection:
            try:
                if retries > 0:
                    print('Will retry to connect to Solr in {} second(s).'.format(retrytime))
                    time.sleep(retrytime)
                    retrytime = retrytime * 2
                    if retrytime > retrytime_max:
                        retrytime = retrytime_max

                r = requests.post(solr_uri, data=datajson, params=params, headers={'Content-Type': 'application/json'})

                # if bad status code, raise exception
                r.raise_for_status()

                if retries > 0:
                    print('Successfull retried to connect Solr.')

                no_connection = False

            except KeyboardInterrupt:
                raise KeyboardInterrupt

            except requests.exceptions.ConnectionError as e:

                    retries += 1

                    sys.stderr.write("Connection to Solr failed (will retry in {} seconds). Exception: {}\n".format(retrytime, e))

            except requests.exceptions.HTTPError as e:
                # a version conflict is not a temporary error: don't retry the
                # same data, but let the caller read and recompute
                response_text = getattr(e.response, 'text', '') or ''
                if check_version and (e.response.status_code == 409 or (e.response.status_code == 400 and 'version conflict' in response_text.lower())):
                    raise classification_concurrency_error(
                        "Version conflict while posting update: {}".format(
                            response_text))

                if e.response.status_code == 503:

                    retries += 1

                    sys.stderr.write("Solr temporary unavailable (HTTP status code 503). Will retry in {} seconds). Exception: {}\n".format(retrytime, e))

                else:
                    no_connection = False

                    sys.stderr.write('Error while posting data to Solr: {}'.format(e))

                    raise(e)

            except BaseException as e:
                no_connection = False

                sys.stderr.write('Error while posting data to Solr: {}'.format(e))

                raise(e)


    # tag a document by adding new value to field
    def tag(self, docid=None, field=None, value=None, data=None):
        if data is None:
            data = {}

        data_merged = data.copy()

        if docid:
            data_merged['id'] = docid

        if field:
            if field in data_merged:
                # if not list, convert to list
                if not isinstance(data_merged[field], list):
                    data_merged[field] = [data_merged[field]]
                # add value to field
                data_merged[field].append(value)
            else:
                data_merged[field] = value

        result = self.update(data=data_merged, add=True)

        return result

    # search for documents with query and without tag and update them with the tag
    def update_by_query(self, query, field=None, value=None, data=None, queryparameters=None):
        if data is None:
            data = {}

        import pysolr

        count = 0

        solr = pysolr.Solr(self.solr + self.core)

        #
        # extend query: do not return documents, that are tagged
        #

        query_marked_before = ''

        if field:
            query_marked_before = field + ':"' + solr_mask(value) + '"'

        # else extract field and value from data to build query of yet tagged docs to exclude

        for fieldname in data:

            if isinstance(data[fieldname], list):

                for value in data[fieldname]:

                    if query_marked_before:
                        query_marked_before += " AND "

                    query_marked_before += fieldname + \
                        ':"' + solr_mask(value) + '"'
            else:

                value = data[fieldname]
                if query_marked_before:
                    query_marked_before += " AND "

                query_marked_before += fieldname + \
                    ':"' + solr_mask(value) + '"'

        solrparameters = {
            'fl': 'id',
            'defType': 'edismax',
            'rows': 10000000,
        }

        # add custom Solr parameters (if the same parameter, overwriting the obove defaults)
        if queryparameters:
            solrparameters.update(queryparameters)

        if query_marked_before:
            # don't extend query but use filterquery for more performance (cache) on aliases
            solrparameters["fq"] = 'NOT (' + query_marked_before + ')'

        if self.verbose:
            print("Solr query:")
            print(query)
            print("Solr parameters:")
            print(solrparameters)

        results = solr.search(query, **solrparameters)

        for result in results:
            docid = result['id']

            if self.verbose:
                print("Tagging {}".format(docid))

            self.tag(docid=docid, field=field, value=value, data=data)

            count += 1

        return count

    def get_data(self, docid, fields):

        uri = self.solr + self.core + '/get?id=' + \
            urllib.parse.quote(docid) + '&fl=' + ','.join(fields)

        request = urllib.request.urlopen(uri)
        encoding = request.info().get_content_charset('utf-8')
        data = request.read()
        request.close()

        solr_doc = json.loads(data.decode(encoding))

        data = None
        if 'doc' in solr_doc:
            data = solr_doc['doc']

        return data

    def commit(self):

        uri = self.solr + self.core + '/update?commit=true'
        if self.verbose:
            print("Committing to {}".format(uri))
        request = urllib.request.urlopen(uri)
        request.close()

    #
    # Optional multi source classification sync
    #
    # The same document can be classified by more than one ETL source. Each
    # source repeatedly sends the full list of tags of its latest version (an
    # empty list means the source retracted its classification). The tags of
    # each source are kept as a contribution per document, source and version,
    # so retracting one source keeps the tags still provided by other sources.
    # The query facing field holds the de-duplicated union of all sources.
    #

    # parse the stored state (one JSON value per source) to a dict
    # source -> {'version': ..., 'tags': [...]}
    def parse_classification_state(self, values):

        state = {}

        if values is None:
            return state

        if isinstance(values, str):
            values = [values]

        for value in values:
            if not value:
                continue

            try:
                contribution = json.loads(value)
            except (TypeError, ValueError):
                # ignore foreign or broken values instead of failing the doc
                continue

            if not isinstance(contribution, dict):
                continue

            source = contribution.get('source')
            if not source:
                continue

            if 'version' not in contribution:
                continue

            state[source] = {
                'version': contribution['version'],
                'tags': canonical_tags(contribution.get('tags')),
            }

        return state

    # calculate the new state and the query field from the old state and the
    # contribution of one source.
    # Returns None if the contribution is a replay or older than the stored
    # version (nothing to do).
    # Raises classification_conflict_error on the same version with different
    # content.
    def merge_classification(self, state, source, version, tags):

        tags = canonical_tags(tags)

        if source in state:
            stored = state[source]

            comparison = compare_versions(version, stored['version'])

            if comparison < 0:
                # old version: skip
                return None

            if comparison == 0:
                if tags == stored['tags']:
                    # same version and same content: replay, nothing to write
                    return None

                raise classification_conflict_error(
                    "Source '{}' sent different content for already imported version '{}' (stored: {}, incoming: {})".format(
                        source, version, stored['tags'], tags))

        # same version and same content was handled above, so this is either a
        # new source or a newer version
        new_state = dict(state)
        new_state[source] = {'version': version, 'tags': tags}

        return new_state

    # union of the tags of all sources of a state, de-duplicated
    @staticmethod
    def classification_tags(classifications):

        union = set()

        for contribution in classifications.values():
            union.update(contribution['tags'])

        return sorted(union)

    # serialize state to the values of the state field (one JSON value per
    # source)
    def classification_state_values(self, classifications):

        values = []

        for source in sorted(classifications):
            contribution = classifications[source]
            values.append(json.dumps({
                'source': source,
                'version': contribution['version'],
                'tags': contribution['tags'],
            }, sort_keys=True, separators=(',', ':')))

        return values

    # read the classification state of a document by the realtime get handler
    # returns (state, version, exists): version is the _version_ of the
    # document and exists tells if the document was found
    def get_classification_state(self, docid):

        try:
            solr_doc = self.get_data(
                docid=docid,
                fields=['_version_', self.classification_state_field])
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return {}, None, False
            raise

        if not solr_doc:
            return {}, None, False

        state = self.parse_classification_state(
            solr_doc.get(self.classification_state_field))

        return state, solr_doc.get('_version_'), True

    # set the classification of one source of a document.
    # The source contribution and the query field are committed in one atomic,
    # version checked partial update. On a version conflict the document is
    # read, recomputed and retried a limited count of times. Other fields of
    # the document are not touched.
    def set_classification(self, docid, source, version, tags, commit=None):
        if not source:
            raise ValueError("Classification source is required")

        if version is None:
            raise ValueError("Classification version is required")

        return self.update_classifications(
            docid=docid,
            contributions=[{
                'source': source,
                'version': version,
                'tags': tags,
            }],
            commit=commit)

    # retract the classification of a source: empty tag list at a new version.
    # Tags still provided by other sources are kept.
    def remove_classification(self, docid, source, version, commit=None):

        return self.set_classification(
            docid=docid, source=source, version=version, tags=[],
            commit=commit)

    # apply several contributions (from different sources) to one document.
    # All contributions and the query field are written in the same atomic,
    # version checked partial update, so concurrent writers can not interleave
    # sources of one document.
    def update_classifications(self, docid, contributions, commit=None):
        if not contributions:
            return {'status': 'skipped', 'version': None}

        # validate first, so no source is written if another one is invalid or
        # conflicts
        normalized = []
        for contribution in contributions:
            source = contribution.get('source')
            version = contribution.get('version')

            if not source:
                raise ValueError("Classification source is required")

            if version is None:
                raise ValueError("Classification version is required")

            normalized.append({
                'source': source,
                'version': version,
                'tags': canonical_tags(contribution.get('tags')),
            })

        retries = 0

        while True:

            state, document_version, document_exists = self.get_classification_state(
                docid=docid)

            merged = state
            status = 'skipped'

            for contribution in normalized:
                # a conflict of any source raises before anything is written
                contribution_merged = self.merge_classification(
                    state=merged,
                    source=contribution['source'],
                    version=contribution['version'],
                    tags=contribution['tags'],
                )

                if contribution_merged is not None:
                    merged = contribution_merged
                    status = 'updated'

            if status == 'skipped':
                if self.verbose:
                    print("Skipping classification of {} (all old versions or replays)".format(
                        docid))
                return {'status': 'skipped', 'version': document_version}

            update_fields = {
                'id': docid,
                self.classification_state_field: {
                    'set': self.classification_state_values(merged)
                },
                self.classification_query_field: {
                    'set': self.classification_tags(merged)
                },
            }

            if document_exists and document_version is not None:
                # only update if the document was not changed since it was
                # read (optimistic concurrency by _version_)
                update_fields['_version_'] = document_version
            elif not document_exists:
                # document must still not exist (otherwise a concurrent
                # writer created it and we have to read and merge again)
                update_fields['_version_'] = 0

            try:
                self.post(data=update_fields, commit=commit,
                          check_version=True)

                return {'status': status, 'version': document_version}

            except classification_concurrency_error:

                retries += 1

                if retries > self.classification_retries:
                    raise classification_concurrency_error(
                        "Could not update classification of {} after {} attempts because of concurrent updates".format(
                            docid, retries))

                if self.verbose:
                    print("Version conflict while updating classification of {} (attempt {}), reading and recomputing again".format(
                        docid, retries))

    def get_lastmodified(self, docid):
        # convert mtime to solr format
        solr_doc_mtime = None

        solr_doc = self.get_data(docid=docid, fields=["file_modified_dt"])

        if solr_doc:
            if 'file_modified_dt' in solr_doc:
                solr_doc_mtime = solr_doc['file_modified_dt']

            # todo: for each plugin
#			solr_meta_mtime = False
#			if 'meta_modified_dt' in solr_doc['doc']:
#				solr_meta_mtime = solr_doc['doc']['meta_modified_dt']

        return solr_doc_mtime

    def delete(self, parameters, docid=None, query=None,):
        import pysolr

        if 'solr' in parameters:
            self.solr = parameters['solr']
            if not self.solr.endswith('/'):
                self.solr += '/'

        if 'index' in parameters:
            self.core = parameters['index']

        solr = pysolr.Solr(self.solr + self.core)

        if docid:
            result = solr.delete(id=docid)

        if query:
            result = solr.delete(q=query)

        return result

    #
    # append synonyms by Solr REST API for managed resources
    #

    def append_synonyms(self, resourceid, synonyms):

        url = self.solr + self.core + '/schema/analysis/synonyms/' + resourceid
        headers = {'content-type': 'application/json'}

        r = requests.post(url=url, data=json.dumps(synonyms), headers=headers)


def solr_mask(string_to_mask, solr_specialchars='\+-&|!(){}[]^"~*?:/'):

    masked = string_to_mask
    # mask every special char with leading \
    for char in solr_specialchars:
        masked = masked.replace(char, "\\" + char)

    return masked

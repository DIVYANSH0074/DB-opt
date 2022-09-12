import logging
import re
from copy import deepcopy
from reg_hub_spoke.collection_constants import UniRegConstants
from reg_hub_spoke.constants import RepoNameConstants
from reg_hub_spoke.adf_resolver.utilities import get_repo_hierarchy, range_enumerator
from reg_hub_spoke.db.operations import DB
from reg_hub_spoke.db.queries import UniregQueries
from reg_hub_spoke.logger.setup import get_logger
from reg_ds.constants import AdfConstants
from reg_ds.homoglyph import homoglyph_resolver

logger = get_logger()


class ADFResolver(object):
    MAX_ITERATIONS = 100

    def __init__(self, repo, adf):
        self.__repo = repo
        self.__adf = adf

    @property
    def adf(self):
        return self.__adf

    @property
    def all_adfs(self):
        return self.generate_adfs(self.adf)

    @property
    def repo(self):
        return self.__repo

    @repo.setter
    def repo(self, repo):
        self.__repo = repo

    def generate_adfs(self, adf):
        result_adf = []
        tmp_adf = dict()
        for key, value in adf.items():
            if not isinstance(value, dict):
                tmp_adf[key] = value

        is_op_exists = False
        for key, item in adf.items():
            if isinstance(item, dict):
                is_op_exists = True
                op = item.get('op')
                if op == 'range':
                    try:
                        values = range_enumerator(item.get('start'), item.get('end'))
                    except Exception as e:
                        logger.warning("Got exception in adf range generator for start: {0}, end: {1}. Error: {2}".format(
                            item.get('start'), item.get('end'), str(e)))
                        return result_adf
                else:
                    values = []
                if not values:
                    continue
                for value in values:
                    copy_adf = tmp_adf.copy()
                    copy_adf[key] = value
                    copy_adf['multiple'] = True
                    result_adf.append(copy_adf)
        if is_op_exists is False:
            result_adf.append(tmp_adf)
        return result_adf

    def construct_regex(self, adf, repo_hierarchy):
        """ construct regex based adf """
        if self.repo == RepoNameConstants.US_PLAW:
            doc_number = adf.get('document_number')
            splitted_law_no = doc_number.split()
            law_no = splitted_law_no[-1]
            splitted_nos = law_no.split('-')
            if len(splitted_nos) != 2:
                return None
            return self.repo + '_' + splitted_nos[0] + '_.*_' + splitted_nos[1] + '$'

        is_end_regex = False
        completed_level = None
        regex = self.repo
        repo_level_hierarchy = sorted(repo_hierarchy, key=lambda i: i['level'])
        for item in repo_level_hierarchy:
            item_type = item.get('type')
            notation = item.get('notation')
            level = item.get('level')
            value = adf.get(item_type)
            if completed_level == level:
                continue
            if value is not None:
                if not is_end_regex:
                    regex += '_'
                if value:
                    regex += notation + '_' + str(value)
                else:
                    regex += notation
                completed_level = level
                is_end_regex = False
            else:
                if is_end_regex is False:
                    regex += '_.*'
                    is_end_regex = True

        return regex[:-3] + '$' if regex[-1] == '*' else regex + '$'

    def get_uid_regex(self, adf):
        repo_hierarchy = get_repo_hierarchy(self.repo)
        try:
            regex = self.construct_regex(adf, repo_hierarchy)
            if regex is None:
                return None
            return '^' + regex
        except Exception as e:
            logger.warning("Got exception while constructing uid regex. Error: {0}".format(str(e)))
            return None

    def get_all_uid_regex(self):
        """ returns list of all uid regex """
        uid_regex = []
        for adf in self.all_adfs:
            uid_regex.append(self.get_uid_regex(adf))
        return uid_regex

    def get_complete_uid(self, adf):
        """ returns complete uid from uid_regex """
        if not adf:
            logger.error("Invalid adf: {0} to construct regex".format(adf))
            return None
        if self.repo == RepoNameConstants.US_FR:
            if 'citation' in adf:
                ret_val, documents = DB.get_documents(UniRegConstants.COLLECTION,
                                                     query={UniRegConstants.CITATION: adf.get('citation'),
                                                            UniRegConstants.REPO: self.repo},
                                                     projection={UniRegConstants.UID: 1})
                if ret_val is False:
                    logger.warning("Failed to resolve adf: {0}".format(adf))
                    return None
                if documents:
                    return [doc.get(UniRegConstants.UID) for doc in documents]
            ret_val, documents = DB.get_documents(UniRegConstants.COLLECTION,
                                                 query={UniRegConstants.VOLUME: float(adf.get(UniRegConstants.VOLUME)),
                                                        UniRegConstants.START_PAGE: {
                                                            "$lte": float(adf.get('page_number', '1'))},
                                                        UniRegConstants.END_PAGE: {
                                                            "$gte": float(adf.get('page_number', '0'))},
                                                        UniRegConstants.REPO: self.repo},
                                                 projection={UniRegConstants.UID: 1})
            if ret_val is False:
                logger.warning("Failed to resolve adf: {0}".format(adf))
                return None
            if documents:
                return [doc.get(UniRegConstants.UID) for doc in documents]
            return None

        if self.repo == RepoNameConstants.US_PLAW:
            if 'alias' in adf:
                adf['alias'] = adf['alias'].replace('–', '-')
                ret_val, document = DB.get_documents(UniRegConstants.COLLECTION,
                                                     query={UniRegConstants.ALIAS: adf.get('alias'),
                                                            UniRegConstants.REPO: self.repo},
                                                     projection={UniRegConstants.UID: 1}, find_one=True)
            else:
                adf['document_number'] = adf['document_number'].replace('–', '-')
                ret_val, document = DB.get_documents(UniRegConstants.COLLECTION,
                                                     query={UniRegConstants.DOCUMENT_NUMBER: adf.get('document_number'),
                                                            UniRegConstants.REPO: self.repo},
                                                     projection={UniRegConstants.UID: 1}, find_one=True)
            if ret_val is False:
                logger.warning("Failed to resolve adf: {0}".format(adf))
                return None
            if document:
                return [document.get(UniRegConstants.UID)]
        try:
            uid_regex = self.get_uid_regex(adf)
            # uid_regex = homoglyph_resolver(uid_regex)
        except Exception as e:
            logger.warning("Got exception while fetching uid regex. Error: {0}".format(str(e)))
            return None
        if uid_regex is None:
            logger.warning("Uid regex is None for adf: {0}".format(self.adf))
            return None

        ret_val, documents = DB.get_documents(UniRegConstants.COLLECTION,
                                              query={UniRegConstants.UID: {'$regex': uid_regex},
                                                     UniRegConstants.REPO: self.repo},
                                              projection={UniRegConstants.UID: 1}, limit=20)
        if ret_val is False:
            logger.warning("Got exception while resolving uid regex: {0}".format(uid_regex))
            return None
        if documents is None:
            logger.warning("Failed to resolve uid regex: {0} for repo: {1}".format(uid_regex, self.repo))
            return None
        if len(documents) == 1:
            return [doc.get(UniRegConstants.UID) for doc in documents]
        if len(documents) == 0:
            logger.warning("Unable to resolve uid regex: {0}".format(uid_regex))
            return None
        if adf.get('multiple') is True:
            return [doc.get(UniRegConstants.UID) for doc in documents]
        logger.warning("Unable to resolve uid regex: {0}. Has multiple matches".format(uid_regex))
        return None

    def get_fr_uid_from_source(self, adf, source_uid):
        """
        param adf: fr adf object
        param source_uid: uid of AuthDoc where FR citation occured
        return: uid of FR document
        Reference doc: https://regology.atlassian.net/wiki/spaces/TR/pages/178585611/Issues+with+FR+citations+source+field
        Given an FR adf, check all the possible uids using page_number.
        If there is only one uid, return that uid.
        If there are multiple uids, then go into the each uid and check if the source_uid is present in the uid.
        """
        if not adf:
            logger.error("Invalid adf: {0} to construct regex".format(adf))
            return None
        if not source_uid:
            logger.error("Invalid source_uid passed: {0}".format(source_uid))
            return None
        if self.repo == RepoNameConstants.US_FR and AdfConstants.PAGE_NUMBER in adf:
            repo = self.repo
            volume = int(adf.get(AdfConstants.VOLUME, -1))
            start_page = {"$lte": int(adf.get(AdfConstants.PAGE_NUMBER, 1))}
            end_page = {"$gte": int(adf.get(AdfConstants.PAGE_NUMBER, 0))}
            query = UniregQueries.repo_volume_start_page_end_page_query(
                repo, volume, start_page, end_page
            )
            documents = UniregQueries.get_docs_by_query(
                query,
                projection={UniRegConstants.UID: 1, UniRegConstants.CFR_REFERENCES: 1},
            )
            if not documents:
                logger.warning("Failed to resolve adf: {0}".format(adf))
                return None

            uids = []
            for doc in documents:
                # Forward link uids from the CFR references metadata.
                fwd_uids = self.get_fr_to_cfr_uids(doc.get(UniRegConstants.CFR_REFERENCES, []))
                uids.extend([doc.get(UniRegConstants.UID) for fwd_uid in fwd_uids if fwd_uid in source_uid])
            return uids
        return None

    def get_fr_to_cfr_uids(self, cfr_references):
        """
        Given a list of CFR citations, returns their uids.
        param cfr_references: cfr_references to get uids from.
        return: list of uids.
        """
        uids = []
        adfs = []
        prev_repo = self.repo
        if cfr_references is not None:
            self.repo = RepoNameConstants.US_ECFR
            for cfr_reference in cfr_references:
                adf = {
                    AdfConstants.TITLE: cfr_reference.get(AdfConstants.TITLE),
                    AdfConstants.PART: cfr_reference.get(AdfConstants.PART),
                    AdfConstants.REPO: RepoNameConstants.US_ECFR,
                }
                if adf.get(AdfConstants.TITLE) is not None and adf.get(AdfConstants.PART) is not None:
                    adfs.append(adf)
            uids = get_all_uids_from_adfs(adfs)
        self.repo = prev_repo
        return uids


def get_all_uids_from_adfs(adfs):
    """ returns list of uids from adfs"""
    uids = []
    for adf in adfs:
        repo = adf.get('repo')
        if repo:
            adf_resolver = ADFResolver(repo, adf)
            for _adf in adf_resolver.all_adfs:
                result = adf_resolver.get_complete_uid(_adf)
                if result:
                    uids += result
    return list(set(uids))


def get_all_uid_repo_from_adf(adfs):
    """ returns list of {uid, repo} from adfs"""
    repo_uids_dict = dict()
    for adf in adfs:
        repo = adf.get('repo')
        if repo:
            adf_resolver = ADFResolver(repo, adf)
            for _adf in adf_resolver.all_adfs:
                result = adf_resolver.get_complete_uid(_adf)
                if result:
                    if repo not in repo_uids_dict:
                        repo_uids_dict[repo] = result
                    else:
                        repo_uids_dict[repo] += result
    for repo, uids in repo_uids_dict.items():
        repo_uids_dict[repo] = list(set(repo_uids_dict[repo]))
    return repo_uids_dict


def get_references_and_repos_from_adfs(adfs):
    reference_repo_dict = dict()
    if not isinstance(adfs, list):
        return reference_repo_dict
    for adf in adfs:
        repo_uids_dict = get_all_uid_repo_from_adf(adf.get('adfs'))
        for key, value in repo_uids_dict.items():
            if key not in reference_repo_dict:
                reference_repo_dict[key] = value
            else:
                reference_repo_dict[key] += value
    for key, value in reference_repo_dict.items():
        reference_repo_dict[key] = list(dict.fromkeys(value))
    return reference_repo_dict

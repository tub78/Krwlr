#!/usr/bin/env python

import os
import sys
import time
import logging
import urllib2
import json
import codecs
import heapq
import itertools

#import scipy as sp


class ApiError(Exception):
    """ Raise if there is an API issue """
    pass

# types of nodes as characterized by route of discovery
KRWLR_USER_FOLLOWER_LT = 40001
KRWLR_FOLLOWED_USER_LT = 40002
KRWLR_FOLLOWED_ITEM_LT = 40003
KRWLR_ITEM_FOLLOWER_LT = 40004

KRWLR_USER_T = [KRWLR_USER_FOLLOWER_LT, KRWLR_FOLLOWED_USER_LT, KRWLR_ITEM_FOLLOWER_LT]
KRWLR_ITEM_T = [KRWLR_FOLLOWED_ITEM_LT]
KRWLR_FOLLOWED_T = [KRWLR_FOLLOWED_USER_LT, KRWLR_FOLLOWED_ITEM_LT]
KRWLR_FOLLOWER_T = [KRWLR_USER_FOLLOWER_LT, KRWLR_ITEM_FOLLOWER_LT]

STATUS_OK = 200
STATUS_NO_CONTENT = 204


# {"item": "", "type": 40001, "id": 583231, "dir": "<", "user": "octocat", "dist": 0}
# {"item": "", "type": 40001, "id": 466804, "dir": "<", "user": "tub78", "dist": 0}
class Link(object):
    """
    Minimal information to identify link

    A link may be created based on lists of:
     1. followed users
     2. user followers
     3. followed items
     4. item followers
    """

    def __init__(self):
        """ Default init """
        self.type = KRWLR_USER_FOLLOWER_LT
        self.id   = 583231
        self.user = u'octocat'
        self.item = u'Hello-World'
        self.dir  = '<'
        self.dist = 0

    def __repr__(self):
        """ Print """
        return json.dumps(self.__dict__)

    def init_json(self, link_type, distance, link_json):
        """ Init from json """
        # type, id
        self.type = link_type
        self.id = link_json.get(u'id')
        self.dist = distance
        # user, item
        if link_type in KRWLR_USER_T:
            self.user = link_json[u'login']
            self.item = u''
        elif link_type in KRWLR_ITEM_T:
            self.user = link_json[u'owner'][u'login']
            self.item = link_json[u'name']
        else:
            self.user = u''
            self.item = u''
        # directionality
        if link_type in KRWLR_FOLLOWED_T:
            self.dir = '>'
        elif link_type in KRWLR_FOLLOWER_T:
            self.dir = '<'
        else:
            self.dir = 'X'
        return(self)

    def parse(self, line):
        """ Init from line """
        mydict = json.loads(line)
        for k, v in mydict.iteritems():
            self.__dict__[k] = v
        #self.type = mydict['type']
        #self.id   = mydict['id']
        #self.user = mydict['user']
        #self.item = mydict['item']
        #self.dir  = mydict['dir']
        #self.dist = mydict['dist']
        return(self)


class Api(object):
    """ Retrieves JSON """

    _max_calls_per_iter = 5
    _url_prefix = u'https://api.github.com'
    _per_page = u'?per_page=100'
    _login_url = u''
    _username = u''
    _password = u''
    _useragent = u''

    def __init__(self, options):
        """ Initialize """
        self._sleep = options.sleep
        self._calls_limit = 0
        self._calls_remaining = 0
        self._fp = None
        # setup useragent
        # login
        fp = self.safe_retrieve_page(u'/users/%s' % KRWLR_DEFAULT_NAME)
        self.update_count()
        logging.info('API> %s' %(repr(self),))

    def __repr__(self):
        """ A string representation """
        dict_copy = self.__dict__.copy()
        dict_copy.pop('_fp')
        return json.dumps(dict_copy)

    def cleanup(self):
        """ logout from network """
        logging.info('API> %s' %(repr(self),))

    def is_exhausted(self):
        """ Are there enough API calls to crawl another page? """
        return(self._calls_remaining < self._max_calls_per_iter)

    def update_count(self):
        """ Update counts based on response header data """
        hdr = self._fp.info()
        self._calls_limit = int(hdr['X-RateLimit-Limit'])
        self._calls_remaining = int(hdr['X-RateLimit-Remaining'])

    def response_check(self):
        """ Return silently, or issue a ApiError if there is trouble """
        success = True
        msg = 'API failure:'
        code = self._fp.getcode()
        if not code in [STATUS_OK]:
            success = False
            msg += ' - status code: %d' % (code,)
        # ... other checks
        # hdr = self._fp.info(); status = hdr['status']
        if not success:
            logging.info(msg)
            raise ApiError(msg)

    def safe_retrieve_additional_pages(self):
        """ Return url for additional pages, if API pagination is active """
        url = None
        try:
            header = self._fp.info()
            if 'Link' in header:
                link_header = header['Link']
                while link_header:
                    (named_link, sep, link_header) = link_header.partition(',')
                    (link, sep, name) = named_link.partition(';')
                    if name.find('rel="next"') >= 0:
                        url = link.strip(' <>')
                        break
        except Exception as error_message:
            logging.info('API!> %s', (error_message,))
            logging.info('API> pagination failed ... returning None')
            url = None
        return(url)

    def safe_retrieve_page(self, url=''):
        """ Retrieve URL and check that response looks ok """
        url = self._url_prefix + url + self._per_page
        json_list = []
        while True:
            logging.info(u'GH_API> fetching url : %s' % (url,))
            time.sleep(self._sleep / float(1e6))
            self._fp = urllib2.urlopen(url)
            self.response_check()
            json_page = json.load(self._fp)
            if not isinstance(json_page, list):
                json_page = [json_page]
            json_list += json_page
            url = self.safe_retrieve_additional_pages()
            if url is None:
                break
        return(json_list)

    def crawl(self, gh_node):
        """ Crawl a page (user or item) for followers and following lists """
        links = []
        dist = gh_node.dist + 1
        if gh_node.type in KRWLR_USER_T:
            info = self.safe_retrieve_page(u'/users/%s' \
                    % (gh_node.user,))
            links += map(lambda RR: \
                    Link().init_json(KRWLR_USER_FOLLOWER_LT, dist, RR), \
                    self.safe_retrieve_page(u'/users/%s/followers' \
                    % (gh_node.user,)))
            links += map(lambda RR: \
                    Link().init_json(KRWLR_FOLLOWED_USER_LT, dist, RR), \
                    self.safe_retrieve_page(u'/users/%s/following' \
                    % (gh_node.user,)))
            links += map(lambda RR: \
                    Link().init_json(KRWLR_FOLLOWED_ITEM_LT, dist, RR), \
                    self.safe_retrieve_page(u'/users/%s/watched' \
                    % (gh_node.user,)))
            #
        else:
            info = self.safe_retrieve_page(u'/repos/%s/%s' \
                    % (gh_node.user, gh_node.item))
            links += map(lambda RR: \
                    Link().init_json(KRWLR_ITEM_FOLLOWER_LT, dist, RR), \
                    self.safe_retrieve_page(u'/repos/%s/%s/watchers' \
                    % (gh_node.user, gh_node.item)))
            links += map(lambda RR: \
                    Link().init_json(KRWLR_FOLLOWED_USER_LT, dist, RR), \
                    self.safe_retrieve_page(u'/repos/%s/%s/collaborators' \
                    % (gh_node.user, gh_node.item)))
            #
        self.update_count()
        return(info, links)


class Krwlr(object):
    """
    Download network of user-user, and user-item connectivity.

    Krwlr is initialized with seed information from a file on disk.  As
    the program runs, a database of users, items, links, unexplored-
    nodes, failed-nodes, and node-hits, is constructed.  The database is
    periodically saved to disk, in case there are failures.  All data is saved
    to a specified download folder.  If the download folder is "DL", the files
    are named:

      1) [DL/seed] seeds
      4) [DL/user] user data
      5) [DL/item] item data
      5) [DL/fail] failed nodes
      2) [DL/hits] user/item hits
      3) [DL/link] links

    Krwlr is for personal and non-profit use.
    """

    def __init__(self, options):
        """ Init """
        self._api = Api(options)
        self._max_download  = options.max_download
        self._max_distance  = options.max_distance
        self._sleep         = options.sleep
        self._save_period   = options.save_period
        self._max_cfailure  = options.max_cfailure
        self.db_init(options)

    def cleanup(self):
        """ Clean up data structures """
        self._api.cleanup()
        self.db_save()
        self.db_cleanup()
        self.db_stats()

    def db_init(self, options):
        """ Set up DB """
        self._seed_queue    = []
        self._user_hits_map = {}
        self._item_hits_map = {}
        # TODO: (should?) apply correction, since some entries included in seeds file
        # TODO: numpy array for seeds
        # TODO: sparse array for hits
        # TODO: sparse matrix for links
        #self._link_mat      = sp.sparse.lil_matrix()
        #
        self._download_dir   = options.download_dir
        self._seed_file      = os.path.join(options.download_dir, 'seed.dat')
        self._fail_file      = os.path.join(options.download_dir, 'fail.dat')
        self._user_hits_file = os.path.join(options.download_dir, 'user_hits.dat')
        self._item_hits_file = os.path.join(options.download_dir, 'item_hits.dat')
        self._user_file      = os.path.join(options.download_dir, 'user.dat')
        self._item_file      = os.path.join(options.download_dir, 'item.dat')
        self._link_file      = os.path.join(options.download_dir, 'link.dat')

        # check existence of critical files
        if not os.path.exists(self._download_dir) \
                or not os.path.exists(self._seed_file):
            msg = 'DB!> Missing download directory %s and/or seed file %s'
            logging.error(msg % (self._download_dir, self._seed_file))
            sys.exit(1)

        # check for restart
        restart = False
        if options.reset == False \
                and os.path.exists(self._user_hits_file) \
                and os.path.exists(self._item_hits_file) \
                and os.path.exists(self._user_file) \
                and os.path.exists(self._item_file) \
                and os.path.exists(self._link_file):
            logging.info('\nDB> Restarting ...')
            restart = True
        else:
            logging.info('\nDB> Resetting ...')
            self.db_reset()

        # load seed and hits
        self.db_load(restart)

        # open files to appending: fail, user, item, and link
        try:
            self._fail_file_fp = codecs.open(self._fail_file, 'a', 'utf-8')
            self._user_file_fp = codecs.open(self._user_file, 'a', 'utf-8')
            self._item_file_fp = codecs.open(self._item_file, 'a', 'utf-8')
            self._link_file_fp = codecs.open(self._link_file, 'a', 'utf-8')
        except Exception as error_message:
            logging.info('DB!> %s', (error_message,))
            msg = 'DB!> Trouble opening files; user: %s, item: %s, or link: %s'
            logging.error(msg % \
                    (self._user_file, self._item_file, self._link_file))
            sys.exit(1)

    def db_reset(self):
        """ Reset DB """
        if os.path.exists(self._fail_file):
            os.remove(self._fail_file)
        if os.path.exists(self._user_hits_file):
            os.remove(self._user_hits_file)
        if os.path.exists(self._item_hits_file):
            os.remove(self._item_hits_file)
        if os.path.exists(self._user_file):
            os.remove(self._user_file)
        if os.path.exists(self._item_file):
            os.remove(self._item_file)
        if os.path.exists(self._link_file):
            os.remove(self._link_file)

    def db_load(self, restart):
        """ Load DB """
        logging.info('\nDB> Loading seed and hits ...')
        # seeds
        num_seeds = 0
        try:
            with codecs.open(self._seed_file, 'r', 'utf-8') as fp:
                for line in fp:
                    if self.db_push_link(Link().parse(line)):
                        num_seeds += 1
        except Exception as error_message:
            logging.info('DB!> %s', (error_message,))
            logging.error('DB!> Unable to read seed file: %s' % \
                    (self._seed_file,))
            sys.exit(1)
        logging.info('DB> Loaded %d unique seeds' % (num_seeds, ))
        # user hits
        if restart:
            num_user_nodes = 0
            num_item_nodes = 0
            try:
                with codecs.open(self._user_hits_file, 'r', 'utf-8') as fp:
                    for line in fp:
                        uid, hts = map(lambda tt: int(tt), line.split())
                        self._user_hits_map[uid] = hts
                        num_user_nodes += 1
                with codecs.open(self._item_hits_file, 'r', 'utf-8') as fp:
                    for line in fp:
                        uid, hts = map(lambda tt: int(tt), line.split())
                        self._item_hits_map[uid] = hts
                        num_item_nodes += 1
            except Exception as error_message:
                logging.info('DB!> %s', (error_message,))
                logging.error('DB!> Unable to read hits file: %s, or %s' \
                        % (self._user_hits_file, self._item_hits_file))
                sys.exit(1)
            logging.info('DB> Loaded %d reachable user nodes' % (num_user_nodes, ))
            logging.info('DB> Loaded %d reachable item nodes' % (num_item_nodes, ))

    def db_save(self):
        """ Save DB to disk """
        logging.info('\nDB> Saving seeds and hits ...')
        # seeds
        num_seeds = 0
        try:
            with codecs.open(self._seed_file, 'w', 'utf-8') as fp:
                for gh_node in self.db_iter_links():
                    fp.write('%s\n' % (repr(gh_node), ))
                    num_seeds += 1
        except Exception as error_message:
            logging.info('DB!> %s', (error_message,))
            logging.error('DB!> Unable to write seeds file: %s' \
                    % (self._seed_file,))
            sys.exit(1)
        logging.info('DB> Saved %d unique seeds' % (num_seeds, ))
        # hits
        num_user_nodes = 0
        num_item_nodes = 0
        try:
            with codecs.open(self._user_hits_file, 'w', 'utf-8') as fp:
                for (uid, hts) in self._user_hits_map.iteritems():
                    fp.write('%d %d\n' % (uid, hts))
                    num_user_nodes += 1
            with codecs.open(self._item_hits_file, 'w', 'utf-8') as fp:
                for (uid, hts) in self._item_hits_map.iteritems():
                    fp.write('%d %d\n' % (uid, hts))
                    num_item_nodes += 1
        except Exception as error_message:
            logging.info('DB!> %s', (error_message,))
            logging.error('DB!> Unable to write hits files: %s, or %s' \
                    % (self._user_hits_file, self._item_hits_file,))
            sys.exit(1)
        logging.info('DB> Saved %d reachable user nodes' % (num_user_nodes, ))
        logging.info('DB> Saved %d reachable item nodes' % (num_item_nodes, ))

    def db_stats(self):
        """ Print DB summary stats """
        logging.info('\nDB> FILE SIZES:')
        try:
            logging.info('      user = %7.1fmb' % (os.stat(self._user_file).st_size/float(pow(2, 20)),))
            logging.info('      item = %7.1fmb' % (os.stat(self._item_file).st_size/float(pow(2, 20)),))
            logging.info('      fail = %7.1fmb' % (os.stat(self._fail_file).st_size/float(pow(2, 20)),))
            logging.info('      link = %7.1fmb' % (os.stat(self._link_file).st_size/float(pow(2, 20)),))
            logging.info('      seed = %7.1fmb' % (os.stat(self._seed_file).st_size/float(pow(2, 20)),))
            logging.info(' user_hits = %7.1fmb' % (os.stat(self._user_hits_file).st_size/float(pow(2, 20)),))
            logging.info(' item_hits = %7.1fmb' % (os.stat(self._item_hits_file).st_size/float(pow(2, 20)),))
        except Exception as error_message:
            logging.info('DB!> %s', (error_message,))
            logging.error('DB!> Unable to stat DB files\n')

    def db_cleanup(self):
        """ Close DB """
        # open user, item and link files to append
        try:
            self._fail_file_fp.close()
            self._user_file_fp.close()
            self._item_file_fp.close()
            self._link_file_fp.close()
        except Exception as error_message:
            logging.info('DB!> %s', (error_message,))
            msg = 'DB!> Trouble closing files; fail %s, user: %s, item: %s, or link: %s'
            logging.error(msg % \
                    (self._fail_file, self._user_file, self._item_file, self._link_file))
            sys.exit(1)

    def db_save_node(self, gh_node, node_info):
        """ Add node to DB """
        if gh_node.type in KRWLR_USER_T:
            fp = self._user_file_fp
        else:
            fp = self._item_file_fp
        try:
            fp.write('%d ' % (gh_node.id,))
            json.dump(node_info, fp)
            fp.write('\n')
        except Exception as error_message:
            logging.info('DB!> %s', (error_message,))
            logging.error('DB!> Unable to append to node file: %s' \
                    % (fp.name,))
            self.cleanup()
            sys.exit(1)

    def db_save_failed_node(self, gh_node):
        """ Writes failed nodes to DB """
        fp = self._fail_file_fp
        try:
            fp.write('%s\n' % (repr(gh_node), ))
        except Exception as error_message:
            logging.info('DB!> %s', (error_message,))
            logging.error('DB!> Unable to append failed file: %s' \
                    % (self._fail_file,))
            self.cleanup()
            sys.exit(1)

    def db_save_links(self, gh_node, gh_links):
        """ Add links to DB """
        fp = self._link_file_fp
        try:
            fp.write('%d' % (gh_node.id,))
            for gh_lnk in iter(gh_links):
                fp.write(' %s:%d' % (gh_lnk.dir, gh_lnk.id))
            fp.write('\n')
        except Exception as error_message:
            logging.info('DB!> %s', (error_message,))
            logging.error('DB!> Unable to append to links file: %s' \
                    % (self._link_file,))
            self.cleanup()
            sys.exit(1)

    def db_pop_link(self):
        """ Return the next seed to crawl """
        try:
            # gh_node = self._seed_queue.pop(0)
            gh_node = heapq.heappop(self._seed_queue)[1]
        except IndexError:
            gh_node = None
        return gh_node

    def db_push_link(self, gh_node):
        """
        Adds ID to reachable set

        Returns true if ID is new
        """
        #val = self._user_hits_map[gh_node.id] \
        #        = self._user_hits_map.setdefault(gh_node.id, 0) + 1
        #val = self._item_hits_map[gh_node.id] \
        #        = self._item_hits_map.setdefault(gh_node.id, 0) + 1
        #self._seed_queue.append(gh_node)
        insert = False
        if gh_node.type in KRWLR_USER_T:
            if gh_node.dist < self._user_hits_map.setdefault(gh_node.id, sys.maxint):
                self._user_hits_map[gh_node.id] = gh_node.dist
                insert = True
        else:
            if gh_node.dist < self._item_hits_map.setdefault(gh_node.id, sys.maxint):
                self._item_hits_map[gh_node.id] = gh_node.dist
                insert = True
        if insert:
            heapq.heappush(self._seed_queue, (gh_node.dist, gh_node))
            return True
        else:
            return False

    def db_push_links(self, gh_links):
        """ Add links to queue """
        map(lambda gh_node: self.db_push_link(gh_node), gh_links)

    def db_iter_links(self):
        """ Iterate through links, without affecting data structure """
        return itertools.imap(lambda x: x[1], self._seed_queue)

    def crawl(self):
        """ Crawl """
        iteration = 0
        num_downloaded = 0
        num_cfailures = 0
        while True:
            if num_downloaded >= self._max_download:
                logging.info('Krwlr>  Reached max download!!')
                break
            if self._api.is_exhausted():
                logging.info('Krwlr>  Api is exhausted!!')
                break
            # next link
            gh_node = self.db_pop_link()
            if not gh_node:
                logging.info('Krwlr>  No more nodes to explore!!')
                break
            if gh_node.dist > self._max_distance:
                logging.info('Krwlr>  Reached max distance!!')
                self.db_push_link(gh_node)
                break
            iteration += 1
            try:
                # retrieve node info and links
                node_info, gh_links = self._api.crawl(gh_node)
            except (ApiError, urllib2.HTTPError) as error_message:
                logging.info('Krwlr!> %s', (error_message,))
                self.db_save_failed_node(gh_node)
                num_cfailures += 1
                if num_cfailures > self._max_cfailure:
                    logging.info('Krwlr>  Reached max failures!!')
                    break
                continue
            num_cfailures = 0
            num_downloaded += 1
            # process info
            self.db_save_node(gh_node, node_info)
            # process links
            self.db_save_links(gh_node, gh_links)
            self.db_push_links(gh_links)
            # diagnostics
            if 0 == (iteration % self._save_period):
                self.db_save()
            time.sleep(self._sleep / float(1e6))


# usage: python FILE.py --test
# instead of: python -m doctest -v FILE.py
def _test():
    import doctest
    doctest.testmod(verbose=True)


#
def main(args=None):
    """ """

    import sys
    import argparse

    parser = argparse.ArgumentParser(description='Download network of user-user, and user-item connectivity', epilog='Krwlr is for personal and non-profit use')

    parser.add_argument('--test'         , default=False     , action='store_true' , help=argparse.SUPPRESS)
    parser.add_argument('-v', '--verbosity' , default=-1     , type=int            , help='Level of verbosity for logging', \
            choices=range(-1, 6))

    parser.add_argument('--max_download' , default=10        , type=int            , help='Maximum pages to visit')
    parser.add_argument('--max_distance' , default=1         , type=int            , help='Maximum graph distance to traverse')
    parser.add_argument('--max_cfailure' , default=10        , type=int            , help='Maximum allowed number of consecutive Api failures')
    parser.add_argument('--download_dir' , default='results' , type=str            , help='Directory where files containing crawl data are stored')

    parser.add_argument('--sleep'        , default=1500000   , type=int            , help='Time (ms) to pause between Api calls')
    parser.add_argument('--save_period'  , default=10        , type=int            , help='Number of iterations between backups')
    parser.add_argument('--reset'        , default=False     , action='store_true' , help='Delete crawl data except seed file')

    if args is None:
        args = sys.argv[1:]

    options, extra_args = parser.parse_known_args(args)

    if options.verbosity >= 0:
        logging.basicConfig(level={0: logging.CRITICAL, 1: logging.ERROR, 2: logging.WARNING, 3: logging.INFO, 4: logging.DEBUG, 5: logging.NOTSET}[options.verbosity])
    else:
        logging.basicConfig(level=logging.INFO)

    #logging.disable({0: logging.CRITICAL, 1: logging.ERROR, 2: logging.WARNING, 3: logging.INFO, 4: logging.DEBUG, 5: logging.NOTSET}[options.verbosity])

    if options.test:
        _test()
        return

    crawler = Krwlr(options)
    try:
        crawler.crawl()
    except Exception as error_message:
        logging.info('API!> %s', (error_message,))
        sys.exit(1)
    crawler.cleanup()


#
if __name__ == '__main__':
    main()

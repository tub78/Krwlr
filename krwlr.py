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
import abc
import argparse

#import scipy as sp



class ApiError(Exception):
    """ Raise if there is an API issue """
    pass


class BasicApi(object):
    """ Retrieves JSON """
    __metaclass__ = abc.ABCMeta

    # types of nodes as characterized by route of discovery
    USER_FOLLOWER_LT = 40001
    FOLLOWED_USER_LT = 40002
    FOLLOWED_ITEM_LT = 40003
    ITEM_FOLLOWER_LT = 40004
    STATUS_OK = 200
    STATUS_NO_CONTENT = 204

    def __init__(self, link_class, options):
        """ Initialize """
        self._link_c = link_class
        self._sleep = options.sleep
        self._fp = None

    def __repr__(self):
        """ A string representation """
        dict_copy = self.__dict__.copy()
        dict_copy.pop('_link_c')
        dict_copy.pop('_fp')
        return json.dumps(dict_copy)

    def get_fp(self):
        return self._fp

    def set_fp(self, fp):
        self._fp = fp

    def cleanup(self):
        """ logout from network """
        logging.info('API> %s' % (repr(self),))

    def is_exhausted(self):
        """ Are there enough API calls to crawl another page? """
        return(self.calls_remaining() <= 0)

    def safe_retrieve_page(self, url=''):
        """ Retrieve URL and check that response looks ok """
        msg = 'API failure:'
        success = True
        logging.info(u'API> fetching url : %s' % (url,))
        time.sleep(self._sleep / float(1e6))
        self._fp = urllib2.urlopen(url)
        code = self.get_fp().getcode()
        if not code in [BasicApi.STATUS_OK]:
            success = False
            msg += ' - status code: %d' % (code,)
        # ... other checks
        # hdr = self.get_fp().info(); status = hdr['status']
        if not success:
            logging.info(msg)
            raise ApiError(msg)
        return self.get_fp()

    @staticmethod
    def user_type():
        return [BasicApi.USER_FOLLOWER_LT, BasicApi.FOLLOWED_USER_LT, BasicApi.ITEM_FOLLOWER_LT]

    @staticmethod
    def item_type():
        return [BasicApi.FOLLOWED_ITEM_LT]

    @staticmethod
    def followed_type():
        return [BasicApi.FOLLOWED_USER_LT, BasicApi.FOLLOWED_ITEM_LT]

    @staticmethod
    def follower_type():
        return [BasicApi.USER_FOLLOWER_LT, BasicApi.ITEM_FOLLOWER_LT]

    @abc.abstractmethod
    def calls_remaining(self):
        return

    @abc.abstractmethod
    def get_info(self, node):
        return

    @abc.abstractmethod
    def get_user_followers(self, node):
        return

    @abc.abstractmethod
    def get_followed_users(self, node):
        return

    @abc.abstractmethod
    def get_item_followers(self, node):
        return

    @abc.abstractmethod
    def get_followed_items(self, node):
        return

    def crawl(self, node):
        """ Crawl a page (user or item) for followers and following lists """
        info = None
        links = []
        info = self.get_info(node)
        links += self.get_user_followers(node)
        links += self.get_followed_users(node)
        links += self.get_item_followers(node)
        links += self.get_followed_items(node)
        map(lambda lnk: lnk.set_distance(node.get_distance() + 1), links)
        return(info, links)


class BasicLink(object):
    """
    Minimal information to identify link

    A link may be created based on lists of:
     1. followed users
     2. user followers
     3. followed items
     4. item followers
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, link_type=BasicApi.USER_FOLLOWER_LT, link_id=583231, distance=0):
        """ Default init """
        self.type = link_type
        self.id = link_id
        self.dist = distance

    def __repr__(self):
        """ Print """
        return json.dumps(self.__dict__)

    def set_type(self, new_type):
        self.type = new_type

    def get_type(self):
        return self.type

    def set_id(self, new_id):
        self.id = new_id

    def get_id(self):
        return self.id

    def set_distance(self, new_dist):
        self.dist = new_dist

    def get_distance(self):
        return self.dist

    def get_direction(self):
        """ directionality """
        if self.type in BasicApi.followed_type():
            direction = '>'
        else:
            direction = '<'
        return direction

    def parse_line(self, line):
        """ Init from line """
        mydict = json.loads(line)
        for k, v in mydict.iteritems():
            self.__dict__[k] = v
        return self


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

    def __init__(self, api_class, link_class, options):
        """ Init """
        self._api_c = api_class
        self._link_c = link_class
        self._max_download  = options.max_download
        self._max_distance  = options.max_distance
        self._sleep         = options.sleep
        self._save_period   = options.save_period
        self._max_cfailure  = options.max_cfailure
        self._api           = self._api_c(self._link_c, options)
        logging.info('Krwl> Api: %s' % (repr(self._api),))
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
                    if self.db_push_link(self._link_c().parse_line(line)):
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
                for node in self.db_iter_links():
                    fp.write('%s\n' % (repr(node), ))
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
        denom = float(pow(2, 20))
        try:
            logging.info('      user = %7.1fmb' \
                    % (os.stat(self._user_file).st_size / denom,))
            logging.info('      item = %7.1fmb' \
                    % (os.stat(self._item_file).st_size / denom,))
            logging.info('      fail = %7.1fmb' \
                    % (os.stat(self._fail_file).st_size / denom,))
            logging.info('      link = %7.1fmb' \
                    % (os.stat(self._link_file).st_size / denom,))
            logging.info('      seed = %7.1fmb' \
                    % (os.stat(self._seed_file).st_size / denom,))
            logging.info(' user_hits = %7.1fmb' \
                    % (os.stat(self._user_hits_file).st_size / denom,))
            logging.info(' item_hits = %7.1fmb' \
                    % (os.stat(self._item_hits_file).st_size / denom,))
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

    def db_save_node(self, node, node_info):
        """ Add node to DB """
        if node.get_type() in BasicApi.user_type():
            fp = self._user_file_fp
        else:
            fp = self._item_file_fp
        try:
            fp.write('%d ' % (node.get_id(),))
            json.dump(node_info, fp)
            fp.write('\n')
        except Exception as error_message:
            logging.info('DB!> %s', (error_message,))
            logging.error('DB!> Unable to append to node file: %s' \
                    % (fp.name,))
            self.cleanup()
            sys.exit(1)

    def db_save_failed_node(self, node):
        """ Writes failed nodes to DB """
        fp = self._fail_file_fp
        try:
            fp.write('%s\n' % (repr(node), ))
        except Exception as error_message:
            logging.info('DB!> %s', (error_message,))
            logging.error('DB!> Unable to append failed file: %s' \
                    % (self._fail_file,))
            self.cleanup()
            sys.exit(1)

    def db_save_links(self, node, links):
        """ Add links to DB """
        fp = self._link_file_fp
        try:
            fp.write('%d' % (node.get_id(),))
            for lnk in iter(links):
                fp.write(' %s:%d' % (lnk.get_direction(), lnk.get_id()))
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
            node = heapq.heappop(self._seed_queue)[1]
        except IndexError:
            node = None
        return node

    def db_push_link(self, node):
        """
        Adds ID to reachable set

        Returns true if ID is new
        """
        insert = False
        if node.get_type() in BasicApi.user_type():
            if node.get_distance() < \
                    self._user_hits_map.setdefault(node.get_id(), sys.maxint):
                self._user_hits_map[node.get_id()] = node.get_distance()
                insert = True
        else:
            if node.get_distance() < \
                    self._item_hits_map.setdefault(node.get_id(), sys.maxint):
                self._item_hits_map[node.get_id()] = node.get_distance()
                insert = True
        if insert:
            heapq.heappush(self._seed_queue, (node.get_distance(), node))
            return True
        else:
            return False

    def db_push_links(self, links):
        """ Add links to queue """
        map(lambda node: self.db_push_link(node), links)

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
            node = self.db_pop_link()
            if not node:
                logging.info('Krwlr>  No more nodes to explore!!')
                break
            if node.get_distance() > self._max_distance:
                logging.info('Krwlr>  Reached max distance!!')
                self.db_push_link(node)
                break
            iteration += 1
            try:
                # retrieve node info and links
                node_info, node_links = self._api.crawl(node)
            except (ApiError, urllib2.HTTPError, AttributeError) as error_message:
                logging.info('Krwlr!> %s', (error_message,))
                self.db_save_failed_node(node)
                num_cfailures += 1
                if num_cfailures > self._max_cfailure:
                    logging.info('Krwlr>  Reached max failures!!')
                    break
                continue
            num_cfailures = 0
            num_downloaded += 1
            # process info
            self.db_save_node(node, node_info)
            # process links
            self.db_save_links(node, node_links)
            self.db_push_links(node_links)
            # diagnostics
            if 0 == (iteration % self._save_period):
                self.db_save()
            time.sleep(self._sleep / float(1e6))

    @staticmethod
    def parse_options(args):
        """ Parse options """

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

        options, extra_args = parser.parse_known_args(args)

        return(options, extra_args)


# testing
def _test():
    import doctest
    doctest.testmod(verbose=True)


# main
def main(args=None):
    """ """

    import sys
    if args is None:
        args = sys.argv[1:]

    options, extra_args = Krwlr.parse_options(args)

    if options.verbosity >= 0:
        logging.basicConfig(level={0: logging.CRITICAL, 1: logging.ERROR, 2: logging.WARNING, 3: logging.INFO, 4: logging.DEBUG, 5: logging.NOTSET}[options.verbosity])
    else:
        logging.basicConfig(level=logging.INFO)

    if options.test:
        _test()
        return

    crawler = Krwlr(BasicApi, BasicLink, options)
    try:
        crawler.crawl()
    except Exception as error_message:
        logging.info('%s!> %s' % (__file__, error_message,))
        sys.exit(1)
    crawler.cleanup()

# main:
#       python FILE.py
# doctests:
#       python FILE.py --test
if __name__ == '__main__':
    main()

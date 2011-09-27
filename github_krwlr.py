#!/usr/bin/env python

import logging
import json
from krwlr import BasicLink
from krwlr import BasicApi
from krwlr import Krwlr


# {"item": "", "type": 40001, "id": 583231, "user": "octocat", "dist": 0}
# {"item": "", "type": 40001, "id": 466804, "user": "tub78", "dist": 0}
class GitHubLink(BasicLink):
    """
    Minimal information to identify link
    """
    def __init__(self, link_type=BasicApi.USER_FOLLOWER_LT, link_id=583231, distance=0, user=u'octocat', item=u'Hello-World'):
        """ Default init """
        super(GitHubLink, self).__init__(link_type, link_id, distance)
        self.user = user
        self.item = item

    def parse_json(self, link_json):
        """ Init from JSON """
        # type, id
        self.set_id(link_json.get(u'id'))
        # user, item
        if self.get_type() in BasicApi.user_type():
            self.user = link_json[u'login']
            self.item = u''
        elif self.get_type() in BasicApi.item_type():
            self.user = link_json[u'owner'][u'login']
            self.item = link_json[u'name']
        else:
            self.user = u''
            self.item = u''
        return self


class GitHubApi(BasicApi):

    _url_prefix = u'https://api.github.com'
    _per_page = u'?per_page=100'
    _login_url = u''
    _username = u''
    _password = u''
    _useragent = u''

    def __init__(self, link_class, options):
        """ Initialize """
        super(GitHubApi, self).__init__(link_class, options)
        ## setup useragent
        ## login

    def calls_remaining(self):
        """ Based on response header data """
        try:
            hdr = self.get_fp().info()
            result = int(hdr['X-RateLimit-Remaining'])
        except AttributeError:
            result = 1
        return result

    def next_url(self, endpt=''):
        """ Return url for additional pages, if API pagination is active """
        url = None
        try:
            header = self.get_fp().info()
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
        return url

    def get_json_from_endpt(self, endpt=''):
        """ Retrieve JSON """
        url = self._url_prefix + endpt + self._per_page
        json_list = []
        while url:
            json_page = json.load(self.safe_retrieve_page(url))
            if not isinstance(json_page, list):
                json_page = [json_page]
            json_list += json_page
            url = self.next_url(endpt)
        return json_list

    def get_info(self, node):
        if node.get_type() in self.user_type():
            info = self.get_json_from_endpt(u'/users/%s' \
                    % (node.user,))
        else:
            info = self.get_json_from_endpt(u'/repos/%s/%s' \
                    % (node.user, node.item))
        return info

    def get_user_followers(self, node):
        links = []
        if node.get_type() in self.user_type():
            links += map(lambda RR: \
                    GitHubLink(self.USER_FOLLOWER_LT).parse_json(RR), \
                    self.get_json_from_endpt(u'/users/%s/followers' \
                    % (node.user,)))
        return links

    def get_followed_users(self, node):
        links = []
        if node.get_type() in self.user_type():
            links += map(lambda RR: \
                    GitHubLink(self.FOLLOWED_USER_LT).parse_json(RR), \
                    self.get_json_from_endpt(u'/users/%s/following' \
                    % (node.user,)))
        else:
            links += map(lambda RR: \
                    GitHubLink(self.FOLLOWED_USER_LT).parse_json(RR), \
                    self.get_json_from_endpt(u'/repos/%s/%s/collaborators' \
                    % (node.user, node.item)))
        return links

    def get_item_followers(self, node):
        links = []
        if node.get_type() in self.item_type():
            links += map(lambda RR: \
                    GitHubLink(self.ITEM_FOLLOWER_LT).parse_json(RR), \
                    self.get_json_from_endpt(u'/repos/%s/%s/watchers' \
                    % (node.user, node.item)))
        return links

    def get_followed_items(self, node):
        links = []
        if node.get_type() in self.user_type():
            links += map(lambda RR: \
                    GitHubLink(self.FOLLOWED_ITEM_LT).parse_json(RR), \
                    self.get_json_from_endpt(u'/users/%s/watched' \
                    % (node.user,)))
        return links


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

    crawler = Krwlr(GitHubApi, GitHubLink, options)
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

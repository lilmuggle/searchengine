#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

'a zhihu_question search engine'

import jieba
import sqlite3

class searcher:
  def __init__(self):
	self.con = sqlite3.connect('zhihu.db')

  def __del__(self):
	self.con.close()

  # 查询包含word的所有url的id集合
  def geturllist(self, word):
	# 得到word的id
	wordid = self.getwordid(word)
	if wordid < 0:
		return []
	
	rows = self.con.execute(
	"select urlid from invertedindex where wordid = %d" % wordid).fetchall()
	return [r[0] for r in rows]

  # 由url的id得到其网址和题目
  def geturlandtitle(self, id):
	url = self.con.execute(
	"select url from urllist where rowid = %d" % id).fetchone()[0]
	title = self.con.execute(
	"select title from urltitle where urlid = %d" % id).fetchone()[0]
	
	return title, url

  # 由单词得到其id
  def getwordid(self, word):
	wordrow = self.con.execute(
	"select rowid from wordlist where word = '%s'" % word).fetchone()
	if wordrow == None:
		return -1
	return wordrow[0]
	
  # 对查询分词后进行搜索并排序显示
  def search(self, query):
	query_list = jieba.lcut_for_search(query)
	# 去除分词出来的空格
	query_list = [q for q in query_list if q != ' ']
	if len(query_list) == 0:
		return
	
	# 对被检索出来的url，记录其由query分词得到的所有词检索出来的总次数
	# 打印总次数最高的前10个
	url_hits_cnt = {}
	for query_word in query_list:
		url_list = self.geturllist(query_word)
		for url in url_list:
			if url in url_hits_cnt:
				url_hits_cnt[url] += 1
			else:
				url_hits_cnt[url] = 1
	
	print '**********'
	if len(url_hits_cnt) == 0:
		print u'查询页面不存在 /(ToT)/~~' + '\n'
	sorted_url = sorted(url_hits_cnt.iteritems(), key = lambda x: x[1], reverse = True)
		
	length = min(10, len(url_hits_cnt))
	for i in range(length):
		t, u = self.geturlandtitle(sorted_url[i][0])
		print t
		print u + '\n'
	
if __name__ == "__main__":
	s = searcher()
	
	tmp = jieba.cut("初始化完成")
	
	print ''.join(tmp)
	
	print '+---------------------------------+'
	print u'|           知 乎 一 下           |'
	print '+---------------------------------+'
	
	
	while(True):
		query = raw_input('ZhihuSearch: ')
		s.search(query)
	
#!/usr/bin/env python
# -*- coding: utf-8 -*-

'a zhihu_question crawler'

import re
import time
import jieba
import Queue
import urllib2
import sqlite3
from urlparse import urljoin
from bs4 import BeautifulSoup
from pybloom import BloomFilter

# 中文常见的停用词表
ignorewords = set(['是', '的', '和', '与', '在', '我', '就', '如', '接着'])

class crawler:

  # 初始化爬虫时连接数据库并创建一个布隆过滤器
  def __init__(self):
	self.con = sqlite3.connect('zhihu.db')
	self.bf = BloomFilter(capacity = 100000, error_rate = 0.001)
  
  def __del__(self):
	self.con.close()
	
  # 用于获取条目的id，若其不存在，就将其加入到数据库中
  def getentryid(self, table, field, value, createnew = True):
	cur = self.con.execute(
	"select rowid from %s where %s = '%s'" % (table, field, value))
	res = cur.fetchone()
	if res == None:
		cur = self.con.execute(
		"insert into %s (%s) values ('%s')" % (table, field, value))
		return cur.lastrowid
	else:
		return res[0]

  # 为每个网页建立索引
  def addtoindex(self, url, soup):
	# 判断是否位于question页面
	regex = re.compile(".*www.zhihu.com/question/.*")
	if not regex.match(url):
		return
	print 'Indexing ' + url
  
	title = self.gettitle(soup)	
	# 获取单词
	words = self.cutword(title)
	# 对单词去重
	new_words = list(set(words))
	# 得到url的id
	urlid = self.getentryid('urllist', 'url', url)
	
	# 将题目和每个单词与该url关联
	self.con.execute(
	"insert into urltitle(urlid, title) values (%d, '%s')" % (urlid, title))
	
	for word in new_words:
		if word in ignorewords:
			continue
		wordid = self.getentryid('wordlist', 'word', word)
		self.con.execute(
		"insert into invertedindex(urlid, wordid) values (%d, %d)" % (urlid, wordid))

  # 从HTML中提取出问题的题目
  def gettitle(self, soup):
	return soup.select("#zh-question-title h2 span")[0].text

  # 分词
  def cutword(self, text):
	return jieba.lcut_for_search(text)

	
  # 判断网页是否被索引过
  def isindexed(self, url):
	if url in self.bf:
		return True
	self.bf.add(url)
	return False
  
  # 从5个话题网页开始BFS爬取，并为网页建立索引
  def crawl(self):
	start_seed = [
		'https://www.zhihu.com/topic/19550517/hot',	# 互联网话题热门问答
		'https://www.zhihu.com/topic/19550828/hot',	# 北京话题热门问答
		'http://www.zhihu.com/topic/19550564/hot',	# 阅读话题热门问答
		'https://www.zhihu.com/topic/19551137/hot',	# 美食话题热门问答
		'http://www.zhihu.com/topic/19550994/hot',	# 游戏话题热门问答
		'http://www.zhihu.com/topic/19551388/hot'	# 摄影话题热门问答
	]
	# url队列
	url_queue = Queue.Queue()
	for seed in start_seed:
		url_queue.put(seed)
	while not url_queue.empty():
		url = url_queue.get()
		if self.isindexed(url):
			continue
		try:
			time.sleep(1)
			contents = urllib2.urlopen(url)
		except:
			print "Could not open %s" % url
			continue
		try:
			soup = BeautifulSoup(contents.read(), 'lxml')
			self.addtoindex(url, soup)
			self.con.commit()
			
			links = soup.find_all('a')
			for link in links:
				if 'href' in link.attrs:
					# 只抽取/question/********* 类型的url
					regex = re.compile(".*(/question/[0-9]+).*")
					mat = regex.match(link.attrs['href'])
					if mat:
						url_queue.put(urljoin('https://www.zhihu.com', mat.group(1)))
  
		except Exception, e:
			print e
			print "Could not parse page %s" % url
  
  # 创建数据库表
  def createindextables(self):
	self.con.execute('create table urllist(url)')
	self.con.execute('create table wordlist(word)')
	self.con.execute('create table urltitle(urlid, title)')
	self.con.execute('create table invertedindex(wordid, urlid)')
	
	self.con.execute('create index urlidx on urllist(url)')
	self.con.execute('create index wordidx on wordlist(word)')
	self.con.execute('create index utidx on urltitle(urlid)')
	self.con.execute('create index invertedidx on invertedindex(wordid)')
	
	self.con.commit()
	
if __name__ == "__main__":
	c = crawler()
	c.createindextables()
	c.crawl()

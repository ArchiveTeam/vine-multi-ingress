#!/usr/bin/env ruby

require 'analysand'
require 'uri'
require 'logger'
require 'sucker_punch'

require_relative '../database'
require_relative '../database_operations'
require_relative '../vines_from_url'

$log = Logger.new($stderr)

def logger
  $log
end

include Database
include DatabaseOperations
include VinesFromUrl

class Worker
  include SuckerPunch::Job
  workers 2

  def perform(row)
    doc = row['doc']
    vine_urls = doc['urls']
    tweet_uri = doc['tweet_uri']
    requested_at = doc['requested_at']

    # We can sometimes get a lot more Vines by reading the HTML that Twitter gives us:
    # for example, it includes the tweet context, which may have more Vines in it
    # than what the Twitter listener saw.  Therefore, we use our Vine extraction code
    # and union it with what the Twitter listener found.
    vines_from_url(tweet_uri).each do |p|
      add_video(p.vine_url, requested_at, profile_url: p.profile_url)
    end

    vine_urls.each do |url|
      add_video(url, requested_at)
    end
  end

  def add_video(url, ts, profile_url: nil)
    Post.perform_async(vine_url: url, db: db, credentials: credentials, requested_at: ts, profile_url: profile_url)
  end
end

class Post
  include SuckerPunch::Job
  workers 4

  def perform(*args)
    submit_video_with_retry(*args)
  end
end

twitter_db = Analysand::Database.new(URI(ENV['TWITTER_DB']))
twitter_creds = { username: ENV['TWITTER_USERNAME'], password: ENV['TWITTER_PASSWORD'] }.freeze

v = twitter_db.all_docs({ stream: true, include_docs: true }, twitter_creds)

v.each do |row|
  Worker.perform_async(row)
end

loop do
  if SuckerPunch::Queue.stats.map { |_, sts| sts['jobs']['enqueued'] }.all? { |c| c == 0 }
    break
  else
    sleep 0.5
  end
end

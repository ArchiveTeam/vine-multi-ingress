#!/usr/bin/env ruby

# usage: ./import.rb CSV_FILE
#
# Requires DB_URL, DB_USERNAME, and DB_PASSWORD environment variables to be set.

require 'csv'
require 'time'
require 'logger'
require 'json'
require 'sucker_punch'
require 'concurrent'

require_relative '../database'
require_relative '../database_operations'

$log = Logger.new($stderr)

include Database
include DatabaseOperations

$counter = Concurrent::AtomicFixnum.new(0)

class Post
  include SuckerPunch::Job
  workers 16

  def perform(*args)
    submit_video_with_retry(*args)
    $counter.decrement
  end
end

def add_video(col, ts, profile_url: nil)
  $counter.increment
  Post.perform_async(vine_url: col, db: db, credentials: credentials, requested_at: ts, profile_url: profile_url)
end

def add_videos_from_user_profile(col, ts)
  user_id = col.split('/').last
  template = 'https://vine.co/api/timelines/users/%d?page=%d'
  api_url = format(template, user_id, 1)

  urls = []

  loop do
    $log.info "Fetching #{api_url}"

    out = `curl -s #{api_url} | jq '{ "urls": [.data.records[].permalinkUrl], "next": .data.nextPage }'`
    doc = JSON.parse(out)
    urls += doc['urls']

    if doc['next']
      api_url = format(template, user_id, doc['next'])
    else
      break
    end
  end

  urls.each do |url|
    add_video(url, ts, profile_url: col)
  end
end

def add_videos_from_tweet(tweet_url, ts)
  $log.info "Fetching #{tweet_url}"
  out = `curl -s #{tweet_url}`
  vine_results = out.scan %r{https?://vine.co/#{URI::REGEXP::PATTERN::URIC}+}

  vine_results.uniq.each { |result| interpret_url(result, ts) }
end

def interpret_url(url, ts)
  case url
  when %r{vine.co/v/.+} then
    add_video(url, ts)
  when %r{vine.co/u/.+} then
    add_videos_from_user_profile(url, ts)
  when %r{twitter.com/.+} then
    add_videos_from_tweet(url, ts)
  when %r{\s*} then
    nil
  else
    $log.warn(format("Don't know how to handle input <%s>; skipping", col))
  end
end

CSV.open(ARGV[0], 'r', headers: true).each do |row|
  # The first column is a timestamp.  The other columns contain:
  #
  # - Vine video URLs (vine.co/v/...)
  # - Vine profile URLs (vine.co/u/...)
  # - Tweet URLs (twitter.com/foobar/status/...)
  ts = Time.parse(row[0]).utc.iso8601

  row[1..-1].each { |col| interpret_url(col, ts) }
end

while $counter.value > 0
  sleep 0.1
end

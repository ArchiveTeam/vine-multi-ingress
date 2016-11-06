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
require_relative '../vines_from_url'

$log = Logger.new($stderr)

def logger
  $log
end

include Database
include DatabaseOperations
include VinesFromUrl

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

CSV.open(ARGV[0], 'r', headers: true).each do |row|
  # The first column is a timestamp.  The other columns contain:
  #
  # - Vine video URLs (vine.co/v/...)
  # - Vine profile URLs (vine.co/u/...)
  # - Tweet URLs (twitter.com/foobar/status/...)
  ts = Time.parse(row[0]).utc.iso8601

  row[1..-1].each do |col|
    vines = vines_from_url(col)
    next if vines.empty?

    vines.each do |p|
      add_video(p.vine_url, ts, profile_url: p.profile_url)
    end
  end
end

while $counter.value > 0
  sleep 0.1
end

require 'uri'
require 'concurrent'
require 'sucker_punch'
require 'time'

require_relative '../database'
require_relative '../database_operations'

module Inkdroid
  class Post
    include DatabaseOperations
    include SuckerPunch::Job

    workers 32

    def perform(tweet_id, vine_url, counter, db, credentials)
      submit_video_with_retry(
        tweet_id: tweet_id,
        vine_url: vine_url,
        db: db,
        credentials: credentials)

      counter.decrement
    end
  end

  class Ingester
    include Database

    def perform(url)
      already_done_id = format('inkdroid:%s', url.gsub('/', '_'))

      # Have we already done this file?
      resp = db.head(already_done_id, credentials)
      return if resp.success?

      # No? OK, here we go
      lines = Net::HTTP.get(URI(url)).split("\n")
      puts "read #{url}"
      counter = Concurrent::AtomicFixnum.new(lines.length)

      lines.each do |line|
        tweet_id, vine_url = line.split(/\s+/, 2)
        if !vine_url
          counter.decrement
          next
        end

        # Some URLs have a trailing / that needs to be removed.  Ditto for
        # /embed.
        vine_url.sub!(%r{/$}, '')
        vine_url.sub!(%r{/embed$}, '')

        # Some URLs have an anchor or query at the end that trips us up, so
        # remove it.
        begin
          uri = URI(vine_url)
          uri.fragment = nil
          uri.query = nil

          Post.perform_async(tweet_id, uri.to_s, counter, db, credentials)
        rescue URI::InvalidURIError
          # whatever, skip it and move on
          counter.decrement
        end
      end

      while counter.value > 0
        sleep 1
        print counter.value
        print '...'
      end

      print counter.value
      puts

      db.put(already_done_id, { done_at: Time.now.utc.iso8601 }, credentials)
    end
  end
end

if $0 == __FILE__
  require 'pry'
  Pry.start
end

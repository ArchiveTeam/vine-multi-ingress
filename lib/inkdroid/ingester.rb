require 'sucker_punch'
require 'time'
require 'uri'

require_relative '../database'
require_relative '../database_operations'

module Inkdroid
  class Ingester
    include Database
  end

  class Post
    include DatabaseOperations
    include SuckerPunch::Job
    workers 32

    def perform(tweet_id, vine_url, db, credentials)
      submit_video_with_retry(
        tweet_id: tweet_id,
        vine_url: vine_url,
        db: db,
        credentials: credentials)
    end
  end

  class Ingester
    include Database

    def initialize
      @pending_task_limit = 1000
    end

    def perform(io_or_str)
      io_or_str.each_line do |line|
        line.chomp!
        tweet_id, vine_url = line.split(/\s+/, 2)
        next if !vine_url

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

          Post.perform_async(tweet_id, uri.to_s, db, credentials)

          until can_take_more_tasks?
            sleep 0.01
          end
        rescue URI::InvalidURIError
          # whatever, skip it and move on
          next
        end
      end

      until all_done?
        sleep 1.0
      end
    end

    def can_take_more_tasks?
      queued = SuckerPunch::Queue.stats['Inkdroid::Post']['jobs']['enqueued']
      queued <= @pending_task_limit
    end

    def all_done?
      stats = SuckerPunch::Queue.stats
      queued = stats['Inkdroid::Post']['jobs']['enqueued']
      busy = stats['Inkdroid::Post']['workers']['busy']

      busy == 0 && queued == 0
    end
  end
end

require 'logger'
require 'uri'
require 'time'
require_relative 'ingester'
require_relative '../database'

module Inkdroid
  class HttpIngester
    include Database

    def log
      @logger ||= Logger.new($stderr)
    end

    def perform(url)
      already_done_id = format('inkdroid:%s', url.gsub('/', '_'))

      # Have we already done this file?
      resp = db.head(already_done_id, credentials)
      return if resp.success?

      # No? OK, here we go
      doc = Net::HTTP.get(URI(url))

      begin
        ing = Ingester.new
        ing.perform(doc)
        db.put(already_done_id, { done_at: Time.now.utc.iso8601 }, credentials)
      rescue => e
        log.error(format('%s: %s', url, e.inspect))
      end
    end
  end
end

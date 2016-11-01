require 'securerandom'

module DatabaseOperations
  def submit_video_with_retry(*args)
    submit_video(*args)
  rescue => e
    sleep(rand)
    retry
  end

  def submit_video(tweet_id: nil, tweet_url: nil, vine_url:, now: Time.now.utc.iso8601, db:, credentials:)
    doc_id = format('video:%s', vine_url.gsub('/', '_'))
    doc = { 'url' => vine_url, 'created_at' => now }

    resp = db.put(doc_id, doc, credentials)

    if !(resp.conflict? || resp.success?)
      raise "unhandled response: #{e.inspect}"
    end

    if tweet_id
      db.put!(format('tweet:%s', tweet_id), { tweet_id: tweet_id, vine_url: vine_url }, credentials)
    end

    if tweet_url
      tweet_id = tweet_url.scan(/[0-9]+/).last
      db.put!(format('tweet:%s', tweet_id), { tweet_url: tweet_url, vine_url: vine_url }, credentials)
    end
  end
end

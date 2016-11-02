require 'securerandom'

module DatabaseOperations
  def submit_video_with_retry(*args)
    submit_video(*args)
  rescue => e
    sleep(rand)
    retry
  end

  def submit_video(tweet_id: nil, tweet_url: nil, vine_url:, now: Time.now.utc.iso8601, db:, credentials:)
    vine_slug = vine_url.split('/').last
    doc_id = format('video:%s', vine_slug)
    doc = { 'url' => vine_url, 'created_at' => now }

    resp = db.put(doc_id, doc, credentials)

    if tweet_id
      db.put(format('tweet:%s:%s', tweet_id, vine_slug), { tweet_id: tweet_id, vine_url: vine_url }, credentials)
    end

    if tweet_url
      tweet_id = tweet_url.scan(/[0-9]+/).last
      db.put(format('tweet:%s:%s', tweet_id, vine_slug), { tweet_url: tweet_url, vine_url: vine_url }, credentials)
    end
  end
end

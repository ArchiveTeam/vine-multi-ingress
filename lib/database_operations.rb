require 'securerandom'
require 'logger'

module DatabaseOperations
  def submit_video_with_retry(*args)
    submit_video(*args)
  rescue => e
    puts e
    sleep(rand)
    retry
  end

  def logger
    @logger ||= Logger.new($stderr)
  end
  
  def log(doc_id, resp)
    logger.info(format('PUT %s %d', doc_id, resp.code))
  end

  def submit_video(tweet_id: nil, tweet_url: nil, vine_url:, now: Time.now.utc.iso8601, db:, credentials:, requested_at: nil, profile_url: nil)
    vine_slug = vine_url.split('/').last
    doc_id = format('video:%s', vine_slug)
    doc = { 'url' => vine_url, 'created_at' => now }

    resp = db.put(doc_id, doc, credentials)
    log(doc_id, resp)

    if tweet_id
      db.put(format('tweet:%s:%s', tweet_id, vine_slug), { tweet_id: tweet_id, vine_url: vine_url }, credentials)
    end

    if tweet_url
      tweet_id = tweet_url.scan(/[0-9]+/).last
      db.put(format('tweet:%s:%s', tweet_id, vine_slug), { tweet_url: tweet_url, vine_url: vine_url }, credentials)
    end

    if requested_at
      rt = Time.parse(requested_at).to_i
      db.put(format('request:%s:%d', doc_id, rt), { vine_url: vine_url, requested_at: requested_at }, credentials)
    end

    if profile_url
      user_id = profile_url.split('/').last
      db.put(format('user:%d:%s', user_id, vine_slug), { vine_url: vine_url, profile_url: profile_url }, credentials) 
    end
  end
end

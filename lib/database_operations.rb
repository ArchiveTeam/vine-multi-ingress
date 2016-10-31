module DatabaseOperations
  def submit_video_with_retry(*args)
    submit_video(*args)
  rescue => e
    sleep(rand)
    retry
  end

  def submit_video(tweet_id: nil, tweet_url: nil, vine_url:, now: Time.now.utc.iso8601, db:, credentials:)
    doc_id = format('video:%s', vine_url.gsub('/', '_'))
    tweet_markers = make_tweet_markers(tweet_id, tweet_url)
    doc = {
      'url' => vine_url,
      'tweets' => tweet_markers,
      'created_at' => now
    }

    resp = db.put(doc_id, doc, credentials)

    if resp.success?
      true
    elsif resp.conflict?
      resp = db.get!(doc_id, credentials)
      doc = resp.body

      if !doc['tweets']
        doc['tweets'] = []
      end

      old_length = doc['tweets'].length
      doc['tweets'] |= tweet_markers

      if old_length != doc['tweets'].length
        db.put!(doc_id, doc, $creds)
      end

      true
    else
      raise format('unhandled response: %d', resp.code)
    end
  end

  def make_tweet_markers(tweet_id = nil, tweet_url = nil)
    [].tap do |a|
      if tweet_id
        a << { 'id' => tweet_id }
      end

      if tweet_url
        a << { 'url' => tweet_url }
      end
    end
  end
end

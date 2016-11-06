require 'logger'

module VinesFromUrl
  Pair = Struct.new(:vine_url, :profile_url)

  ##
  # This method handles:
  #
  # Vine video URLs (vine.co/v/...)
  # Vine profile URLs (vine.co/u/...)
  # Tweet URLs (twitter.com/foobar/status/...)
  def vines_from_url(url)
    case url
    when %r{vine.co/v/.+} then
      [Pair.new(url, nil)]
    when %r{vine.co/u/.+} then
      vines_from_user_profile(url).map { |v| Pair.new(v, url) }
    when %r{twitter.com/.+} then
      vine_urls_from_tweet(url).map { |u| vines_from_url(u) }.flatten
    when %r{\s*} then
      []
    else
      logger.warn(format("Don't know how to handle input <%s>; skipping", col))
      []
    end
  end

  def vines_from_user_profile(url)
    user_id = url.split('/').last
    template = 'https://vine.co/api/timelines/users/%d?page=%d'
    api_url = format(template, user_id, 1)

    urls = []

    loop do
      logger.info "Fetching #{api_url}"
      out = `curl -s #{api_url} | jq '{ "urls": [.data.records[].permalinkUrl], "next": .data.nextPage }'`
      doc = JSON.parse(out)
      urls += doc['urls']

      if doc['next']
        api_url = format(template, user_id, doc['next'])
      else
        break
      end
    end

    urls
  end

  def vine_urls_from_tweet(tweet_url)
    logger.info "Fetching #{tweet_url}"
    out = `curl -s #{tweet_url}`
    out.scan(%r{https?://vine.co/#{URI::REGEXP::PATTERN::URIC}+}).uniq
  end
end

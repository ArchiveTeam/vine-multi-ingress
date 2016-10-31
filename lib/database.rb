require 'analysand'

module Database
  def db
    @db ||= Analysand::Database.new(uri)
  end

  def uri
    URI(ENV['DB_URL'])
  end

  def credentials
    @credentials ||= { username: ENV['DB_USERNAME'], password: ENV['DB_PASSWORD'] }.freeze
  end
end

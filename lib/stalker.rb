require 'beanstalk-client'
require 'uri'
require 'timeout'

require 'stalker/errors'
require 'stalker/worker'

module Stalker
  extend self

  JOB_DEFAULTS = {
    :pri => 66536,
    :delay => 0,
    :ttr => 300,
    :max_attempts => 25,
    :retry_delay => 5
  }

  def connect(url)
    @@url = url
    beanstalk
  end

  def enqueue(job, args = {}, opts = {})
    job_handler = jobs[job] || JOB_DEFAULTS
    pri = opts[:pri] || job_handler[:pri]
    delay = opts[:delay] || job_handler[:delay]
    ttr = opts[:ttr] || job_handler[:ttr]
    beanstalk.use(job)
    beanstalk.put([job, args].to_json, pri, delay, ttr)
  rescue Beanstalk::NotConnected => e
    failed_connection(e)
  end

  def job(job_name, options = {}, &block)
    options.reverse_merge!(JOB_DEFAULTS)
    jobs[job_name] = options.merge(:handler => block)
  end
  
  def jobs
    @@jobs ||= {}
  end

  def error(&block)
    add_hook(:error, &block)
  end

  def before(&block)
    add_hook(:before, &block)
  end

  def after(&block)
    add_hook(:after, &block)
  end

  def after_fork(&block)
    add_hook(:after, &block)
  end

  def failed_connection(e)
    log_error exception_message(e)
    log_error "*** Failed connection to #{beanstalk_url}"
    log_error "*** Check that beanstalkd is running (or set a different BEANSTALK_URL)"
    exit 1
  end

  def log(msg)
    puts msg
  end

  def log_error(msg)
    STDERR.puts msg
  end

  def beanstalk
    @@beanstalk ||= Beanstalk::Pool.new(beanstalk_addresses)
  end

  def beanstalk_url
    return @@url if defined?(@@url) and @@url
    ENV['BEANSTALK_URL'] || 'beanstalk://localhost/'
  end

  def beanstalk_addresses
    uris = beanstalk_url.split(/[\s,]+/)
    uris.map {|uri| beanstalk_host_and_port(uri)}
  end

  def beanstalk_host_and_port(uri_string)
    uri = URI.parse(uri_string)
    raise(BadURL, uri_string) if uri.scheme != 'beanstalk'
    "#{uri.host}:#{uri.port || 11300}"
  end

  def exception_message(e)
    msg = [ "Exception #{e.class} -> #{e.message}" ]

    base = File.expand_path(Dir.pwd) + '/'
    e.backtrace.each do |t|
      msg << "   #{File.expand_path(t).gsub(/#{base}/, '')}"
    end

    msg.join("\n")
  end

  def all_jobs
    jobs.keys
  end

  def clear!
    @@jobs = nil
    @@hooks = nil
  end

  def run_hooks(type, *arguments)
    @@hooks[type].each do |block|
      if block.arity == 1
        block.call(arguments.first)
      else
        block.call(*arguments)
      end
    end
  end

  def stats
    out = {}
    beanstalk.list_tubes.each do |server, tubes|
      server_stats = {}
      tubes.each do |tube|
        server_stats[tube] = beanstalk.stats_tube(tube)
      end
      out[server] = server_stats
    end
    out
  end
  
  def inspect
    out = ""
    stats.each do |server, tubes|
      out << "#{server}\n"
      tubes.each do |tube, stats|
        out << "  #{tube}: #{stats["current-jobs-ready"]}\n"
      end
    end
    puts out
  end

  private
    def add_hook(type, &block)
      @@hooks ||= Hash.new { |h, k| h[k] = [] }
      @@hooks[type] << block
    end
end

require 'beanstalk-client'
require 'uri'
require 'timeout'

module Stalker
  extend self
  
  class NoJobsDefined < RuntimeError; end
  class NoSuchJob < RuntimeError; end
  class JobTimeout < RuntimeError; end
  class BadURL < RuntimeError; end

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

  def prep(jobs = nil)
    raise NoJobsDefined unless defined?(@@jobs)

    jobs ||= all_jobs

    jobs.each do |job|
      raise(NoSuchJob, job) unless self.jobs[job]
    end

    log "Working #{jobs.size} jobs: [ #{jobs.join(' ')} ]"

    jobs.each { |job| beanstalk.watch(job) }

    beanstalk.list_tubes_watched.each do |server, tubes|
      tubes.each { |tube| beanstalk.ignore(tube) unless jobs.include?(tube) }
    end
  rescue Beanstalk::NotConnected => e
    failed_connection(e)
  end

  def fork?
    true
  end
  
  def running?
    true
  end

  def start(jobs = nil)
    prep(jobs)
    while running?
      if fork?
        fork_and_work
      else
        work
      end
      GC.start
    end
  end

  def fork_and_work
    pid = fork do
      run_hooks(:after_fork)
      work
    end
    Process.wait(pid)
  end

  def work
    job = beanstalk.reserve
    name, args = JSON.parse(job.body)
    log_job_begin(name, args)
    run_hooks(:before, {name: name, args: args, job: job})
    job_handler = jobs[name]
    raise(NoSuchJob, name) unless job_handler
    job_successful = begin
      Timeout::timeout(job.ttr - 1) do
        job_handler[:handler].call(args, job) || true
      end
    rescue Timeout::Error
      raise JobTimeout, "#{name} hit #{job.ttr-1}s timeout"
    end

    job.delete if job_successful
    log_job_end(name)
    begin
      run_hooks(:after, {name: name, args: args, job: job})
    rescue
      log_error "An error occured while running the after hook: #{exception_message($!)}"
    end
  rescue Beanstalk::NotConnected => e
    failed_connection(e)
  rescue SystemExit
    raise
  rescue => e
    log_error(exception_message(e))
    attempts = job.stats["reserves"]
    if attempts < job_handler[:max_attempts]
      delay = (job_handler[:retry_delay] + attempts ** 3)
      job.release(nil, delay)
      log_job_end(name, "(failed - attempt ##{attempts}, retrying in #{delay}s)") if @job_begun
    else
      job.bury rescue nil
      log_job_end(name, "(failed - attempt ##{attempts}, burying)") if @job_begun
    end

    run_hooks(:error, e, {name: name, args: args, job: job})
  end

  def failed_connection(e)
    log_error exception_message(e)
    log_error "*** Failed connection to #{beanstalk_url}"
    log_error "*** Check that beanstalkd is running (or set a different BEANSTALK_URL)"
    exit 1
  end

  def log_job_begin(name, args)
    args_flat = unless args.empty?
      '(' + args.inject([]) do |accum, (key,value)|
        accum << "#{key}=#{value}"
      end.join(' ') + ')'
    else
      ''
    end

    log [ "Working", name, args_flat ].join(' ')
    @job_begun = Time.now
  end

  def log_job_end(name, message = nil)
    ellapsed = Time.now - @job_begun
    ms = (ellapsed.to_f * 1000).to_i
    log "Finished #{name} in #{ms}ms #{message}"
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

  private
    def add_hook(type, &block)
      @@hooks ||= Hash.new { |h, k| h[k] = [] }
      @@hooks[type] << block
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
end

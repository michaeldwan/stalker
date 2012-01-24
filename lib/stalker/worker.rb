module Stalker
  class Worker
    
    def initialize(jobs = nil)
      jobs ||= Stalker.all_jobs
      
      @fork = ENV["STALKER_FORK_WORKER"] == "true"
      
      raise NoJobsDefined if jobs.blank?

      jobs.each do |job|
        raise(NoSuchJob, job) unless Stalker.jobs[job]
      end

      log "Working #{jobs.size} jobs: [ #{jobs.join(' ')} ]"

      jobs.each { |job| Stalker.beanstalk.watch(job) }

      Stalker.beanstalk.list_tubes_watched.each do |server, tubes|
        tubes.each { |tube| Stalker.beanstalk.ignore(tube) unless jobs.include?(tube) }
      end
      
      handle_signals
    rescue Beanstalk::NotConnected => e
      failed_connection(e)
    end

    def fork?
      @fork
    end

    def start
      loop do
        break if shutdown?
        
        if fork?
          fork_and_work
        else
          work
        end
        GC.start
      end
    end

    def fork_and_work
      @child = fork do
        Stalker.run_hooks(:after_fork)
        work
      end
      Process.wait(@child)
      @child = nil
    end

    def work
      job = Stalker.beanstalk.reserve
      name, args = JSON.parse(job.body)
      log_job_begin(name, args)
      Stalker.run_hooks(:before, {name: name, args: args, job: job})
      job_handler = Stalker.jobs[name]
      raise(NoSuchJob, name) unless job_handler
      job_successful = begin
        Timeout::timeout(job.ttr - 1) do
          job_handler[:handler].call(args, job) || true
        end
      rescue Timeout::Error
        raise JobTimeout, "#{name} hit #{job.ttr-1}s timeout"
      end
      age = job.age
      job.delete if job_successful
      log_job_end(name)
      begin
        ms = ((Time.now - @job_begun).to_f * 1000).to_i
        Stalker.run_hooks(:after, {name: name, args: args, job: job, ms: ms, age: (age * 1000) + ms})
      rescue
        log_error "An error occured while running the after hook: #{Stalker.exception_message($!)}"
      end
    rescue Beanstalk::NotConnected => e
      Stalker.failed_connection(e)
    rescue SystemExit
      raise
    rescue => e
      log_error(Stalker.exception_message(e))
      attempts = job.stats["reserves"]
      if attempts < job_handler[:max_attempts]
        delay = (job_handler[:retry_delay] + attempts ** 3)
        job.release(nil, delay)
        log_job_end(name, "(failed - attempt ##{attempts}, retrying in #{delay}s)") if @job_begun
      else
        job.bury rescue nil
        log_job_end(name, "(failed - attempt ##{attempts}, burying)") if @job_begun
      end

      Stalker.run_hooks(:error, e, {name: name, args: args, job: job})
    end

    def kill_child
      if @child
        log "Killing child at #{@child}"
        if system("ps -o pid,state -p #{@child}")
          Process.kill("KILL", @child) rescue nil
        else
          log_error "Child #{@child} not found, restarting."
          shutdown
        end
      end
    end
    
    def shutdown
      log "Exiting..."
      @shutdown = true
    end
    
    def shutdown!
      shutdown
      kill_child
    end
    
    def shutdown?
      @shutdown
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
      log("Finished #{name} in #{ms}ms #{message}")
    end

    private
      def handle_signals
        %W(INT TERM).each do |sig|
          trap(sig) do
            shutdown? ? (raise Interrupt) : shutdown!
          end
        end
      end

      def log(message)
        Stalker.log(message)
      end
      
      def log_error(message)
        Stalker.log_error(message)
      end
  end
end

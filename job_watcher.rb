module JobWatcher
  def connection
    Backburner::Connection.new(Backburner.configuration.beanstalk_url)
  end

  def remaining_jobs jobs
    if jobs.size > 100
      smaller_jobs = jobs.slice!(0, 100)
      return (jobs | smaller_jobs.map { |j| connection.jobs.find(j) }.compact.map { |j| j.id.to_s }).sort_by(&:to_i)
    else
      return (jobs.map { |j| connection.jobs.find(j) }.compact.map {|j| j.id.to_s}).sort_by(&:to_i)
    end
  end

  def jobs_exhausted? jobs
    remaining_jobs(jobs).empty?
  end

  def queue_priority
      0 # most urgent priority is 0
  end

  def watch additional_args, &block
    jobs = []
    yield(jobs) if block_given?
    job_ids = jobs.compact.map{|j| j[:id]}
    Backburner::Worker.enqueue(self, [*job_ids, additional_args], :delay => 60.seconds)
  end

  def queue
    "JobWatcherQueue"
  end
end

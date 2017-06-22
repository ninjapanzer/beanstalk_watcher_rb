module Job
  class ClientImportWatcher
    extend JobWatcher

    def self.perform(*job_ids, args)
      remaining = remaining_jobs job_ids
      indiff_args = ActiveSupport::HashWithIndifferentAccess.new args
      list_file_id = indiff_args[:list_file_id]
      return Backburner::Worker.enqueue(self, [*remaining, args], delay: 120.seconds) unless remaining.empty?
      ListFile.find(list_file_id).update_attribute :status, ListFile::STATUS_DONE
    end

    def self.queue_priority
      10
    end

    def self.queue_respond_timeout
      2.hours.to_i
    end
  end
end

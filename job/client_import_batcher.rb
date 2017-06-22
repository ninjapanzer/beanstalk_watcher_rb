module Job
  class ClientImportBatcher

    def self.perform(list_file_id)
      list_file = ListFile.find(list_file_id)
      client_id = list_file.client.id
      user_id = list_file.user.id
      lines = []
      Job::ClientImportWatcher.watch queue: 'backburner-jobs', client_id: client_id, user_id: user_id, list_file_id: list_file.id do |jobs|
        phone_number_file = File.new(ListFile.last.upload.path)
        File.open(phone_number_file).each do |line|
          lines << line
          if lines.size >= 10_000
            jobs << Backburner::Worker.enqueue(Job::ClientImportWorker, [*lines, client_id: client_id, user_id: user_id, list_file_id: list_file.id], ttr: 2.hours.to_i)
            lines = []
          end
        end
        if lines.count > 0
          jobs << Backburner::Worker.enqueue(Job::ClientImportWorker, [*lines, client_id: client_id, user_id: user_id, list_file_id: list_file.id], ttr: 2.hours.to_i)
          lines = []
        end

      end
    end

    def self.queue_priority
      100
    end

    def self.queue_respond_timeout
      2.hours.to_i
    end

    def self.queue
      "client-import-batcher"
    end
  end
end

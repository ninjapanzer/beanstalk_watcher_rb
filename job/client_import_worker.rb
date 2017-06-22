module Job
  class ClientImportWorker
    require 'csv'

    def self.perform(*lines, options)
      indiff_options = ActiveSupport::HashWithIndifferentAccess.new options
      client_id = indiff_options[:client_id]
      user_id = indiff_options[:user_id]
      list_file_id = indiff_options[:list_file_id]

      ListFile.find(list_file_id).update_attribute :status, ListFile::STATUS_PENDING

      phone_numbers = []
      now = Time.now
      CSV.parse(lines.join) do |row|
        phone_numbers << PhoneNumber.new(
          area_code: row.first,
          local_part: row.last,
          current: true,
          callable_number: row.join('')
        )
      end
      PhoneNumber.import phone_numbers, {
        on_duplicate_key_update: {
          conflict_target: [:callable_number],
          columns: [ :updated_at ]
        },
        validate: false,
        recursive: false,
        synchronize: phone_numbers
      }

      client_phone_numbers = []
      phone_numbers.each do |pn|
        client_phone_numbers << ClientPhoneNumber.new(
          client_id: client_id,
          exipires_at: now,
          phone_number: pn,
          list_file_id: list_file_id
        )
      end
      ClientPhoneNumber.import client_phone_numbers, {
        on_duplicate_key_update: {
          conflict_target: [:client_id, :phone_number_id],
          columns: [ :updated_at ]
        },
        validate: false,
        recursive: false,
      }
    end

    def self.queue_priority
      10000
    end

    def self.queue_respond_timeout
      2.hours.to_i
    end
  end
end

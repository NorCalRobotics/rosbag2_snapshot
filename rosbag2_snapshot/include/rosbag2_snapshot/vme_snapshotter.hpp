/// @file vme_snapshotter.hpp
/// @author Gabriel Stewart
/// @copyright UPower (c) 2022

#ifndef VME_SNAPSHOTTER_H_INCLUDED
#define VME_SNAPSHOTTER_H_INCLUDED

#include <rosbag2_snapshot/vme_message_queue.hpp>

#if __has_include(<experimental/filesystem>)
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#else
#include <filesystem>
namespace fs = std::filesystem;
#endif
#include <thread>
#include <rosbag2_snapshot/payload_block_math.h>

#include <sys/sem.h>
#include <linux/limits.h>
#include <unistd.h>
#include <string.h>

/// Reserve space for head/tail offsets for each topic, at the end of the namespace
#define TOPIC_OFFSETS_RESERVED_SPACE    (32 << MiB_BITWIDTH)

/// Writing QD of this class
#define ASYNCH_IO_QD   32

#define FILE_FORMAT_IDENTIFER   "VMeRingBuffer\n\0\0"
#define FILE_FORMAT_IDENTIFER_LENGTH   16

typedef union{
    struct{
        char format_identifier[FILE_FORMAT_IDENTIFER_LENGTH];
        struct{
            off64_t tail_offset, head_offset, next_write_offset, ns_size;
            off64_t topic_offsets_offset;
            off64_t topic_offsets_reserved_size;
            off64_t ns_usable_offset, ns_usable_size;
        } linked_list;
        size_t topic_count;
    };
} vme_circular_buffer_file_header_t;

/// Snapshotter class, extended with the ability to store messages on
/// a RAM disk that conforms to the NVMe protocol
template <class TMessageQueue>
class VMeSnapshotter : public Snapshotter<TMessageQueue>
{
  friend TMessageQueue;

public:
  explicit VMeSnapshotter(const rclcpp::NodeOptions & options, fs::path vme_device) :
    Snapshotter<TMessageQueue>(options)
  {
    union semun {
      int              val;
      struct semid_ds *buf;
      unsigned short  *array;
      struct seminfo  *__buf;
    } sem_value;
    int idx;
    bool valid_file;

    // init asynchronous writing semaphore:
    this->sem_key = IPC_PRIVATE;
    this->concurrent_write_semaphore = semget(this->sem_key, 1, 0666 | IPC_CREAT);
    sem_value.val = ASYNCH_IO_QD;
    semctl(this->concurrent_write_semaphore, 0, SETVAL, sem_value);

    // init asynchronous writing thread trackers:
    for(idx = 0; idx < ASYNCH_IO_QD; idx++){
      this->concurrent_writer[idx] = NULL;
    }

    // open the VMe NS file:
    this->vme_ns_filename = vme_device.string();
    this->vme_device_ = fopen64(this->vme_ns_filename .c_str(), "r+b");
    assert(this->vme_device_ != NULL);

    // See if the file is valid
    valid_file = this->read_header();
    if(valid_file){
      if(!this->read_topic_offsets()) valid_file = false;
    }

    if(!valid_file){
      // We are opening an uninitialized VMe device file
      this->init_empty_buffer_header();
      this->write_header();
      auto & buffers = this->get_message_queue_map();
      for(auto & pair : buffers){
        VMeMessageQueue * p_vme_queue = dynamic_cast<VMeMessageQueue *>(pair.second.get());
        // If the queue wasn't created with the right class because virtual/override
        // keywords are not working, then this will be NULL.
        assert(p_vme_queue != NULL);

        if(p_vme_queue->vme_ns_state.offsets == NULL){
          if(this->topic_offsets.find(pair.first) != this->topic_offsets.end()){
            vme_topic_offsets_t empty_topic_offsets = {0, 0};
            this->topic_offsets.emplace(pair.first, empty_topic_offsets);
          }
          p_vme_queue->vme_ns_state.offsets = &(this->topic_offsets[pair.first]);
        }
        if(p_vme_queue->vme_ns_state.lock == NULL){
          p_vme_queue->vme_ns_state.lock = &this->msg_storage_lock_;
        }
      }
    }

    this->open_buffer_state.tail = 0;
    this->open_buffer_state.head = 0;
  }

  ~VMeSnapshotter()
  {
    int idx;

    this->header_.topic_count = this->get_message_queue_map().size();

    // free asynchronous writing thread trackers:
    for(idx = 0; idx < ASYNCH_IO_QD; idx++){
        if(this->concurrent_writer[idx] != NULL){
            delete this->concurrent_writer[idx];
            this->concurrent_writer[idx] = NULL;
        }
    }

    // close the semaphore
    semctl(this->concurrent_write_semaphore, 0, IPC_RMID);

    // update the header and topics' heads/tails
    if(this->vme_device_ == NULL) return;
    this->write_topic_offsets();
    this->write_header();

    // Close open file handles
    fclose(this->vme_device_);
    this->vme_device_ = NULL;
  }

  virtual void init_empty_buffer_header()
  {
    // Set the file format identifier
    strncpy(this->header_.format_identifier, FILE_FORMAT_IDENTIFER, FILE_FORMAT_IDENTIFER_LENGTH);

    // Set the topic count so that we can read the topics' heads/tails (aka footer), too
    this->header_.topic_count = this->get_message_queue_map().size();

    // init FIFO to empty initial state:
    this->header_.linked_list.head_offset = 0;
    this->header_.linked_list.tail_offset = 0;

    // header and footer sizes
    this->header_.linked_list.ns_usable_offset = sizeof(vme_circular_buffer_file_header_t);
    this->header_.linked_list.topic_offsets_reserved_size = TOPIC_OFFSETS_RESERVED_SPACE;

    // How big is the namespace?
    fseeko64(this->vme_device_, 0, SEEK_END);
    this->header_.linked_list.ns_size = ftello64(this->vme_device_);
    std::cout << "VMe NS " << this->vme_ns_filename << ": ";
    if(this->header_.linked_list.ns_size >= ARB_BLOCK_LEN(GiB_BITWIDTH)){
      std::cout << ARB_FULL_BLOCK_COUNT(GiB_BITWIDTH, this->header_.linked_list.ns_size) << "GiB" << std::endl;
    }
    else{
      std::cout << ARB_FULL_BLOCK_COUNT(MiB_BITWIDTH, this->header_.linked_list.ns_size) << "MiB" << std::endl;
    }

    // How much space remains for the circular buffer data?
    this->header_.linked_list.ns_usable_size = this->header_.linked_list.ns_size;
    this->header_.linked_list.ns_usable_size -= this->header_.linked_list.ns_usable_offset;
    this->header_.linked_list.ns_usable_size -= this->header_.linked_list.topic_offsets_reserved_size;

    // Set footer file offset
    this->header_.linked_list.topic_offsets_offset = this->header_.linked_list.ns_size;
    this->header_.linked_list.topic_offsets_offset -= this->header_.linked_list.topic_offsets_reserved_size;
  }

  virtual bool read_header()
  {
    int cmp_result;

    if(! fread64_atomic(this->vme_device_, &this->header_, sizeof(vme_circular_buffer_file_header_t), 0), &(this->header_lock_))
    {
      return false;
    }

    cmp_result = strncmp(this->header_.format_identifier, FILE_FORMAT_IDENTIFER, FILE_FORMAT_IDENTIFER_LENGTH);

    return (cmp_result == 0);
  }

  virtual bool write_header()
  {
    return fwrite64_atomic(this->vme_device_, &this->header_, sizeof(vme_circular_buffer_file_header_t), 0, &(this->header_lock_));
  }

  virtual bool read_topic_offsets()
  {
    char buffer[0x200], * p_buffer = &(buffer[0]), * p_str, * newline_pos;
    size_t idx, byte_count;
    int seek_res;
    std::lock_guard<std::mutex>(this->topic_offsets_lock_);

    if(this->vme_device_ == NULL) return false;
    seek_res = fseeko64(this->vme_device_, this->header_.linked_list.topic_offsets_offset, SEEK_SET);
    if(seek_res != 0) return false;

    for(idx = 0; idx < this->header_.topic_count; idx++){
      TopicDetails topic_info;
      vme_topic_offsets_t current_topic_offsets = {0, 0};

      p_str = fgets(p_buffer, sizeof(buffer), this->vme_device_);
      if(p_str == NULL) return false;

      newline_pos = strchr(p_str, '\n');
      if(newline_pos == NULL) return false;

      *newline_pos = '\0';
      topic_info.name = p_str;

      p_str = fgets(p_buffer, sizeof(buffer), this->vme_device_);
      if(p_str == NULL) return false;

      newline_pos = strchr(p_str, '\n');
      if(newline_pos == NULL) return false;

      *newline_pos = '\0';
      topic_info.type = p_str;

      byte_count = fread(&current_topic_offsets, 1, sizeof(vme_topic_offsets_t), this->vme_device_);
      if(byte_count != sizeof(vme_topic_offsets_t)) return false;

      this->topic_offsets.emplace(topic_info, current_topic_offsets);
      auto p_offsets = &(this->topic_offsets[topic_info]);

      auto p_lock = &this->msg_storage_lock_;
      auto new_vme_state = this->create_queue_vme_ns_state(p_offsets, p_lock);

      this->topic_vme_state.emplace(topic_info, new_vme_state);
    }

    return true;
  }

  virtual bool write_topic_offsets()
  {
    std::string name, type;
    off64_t write_offset;
    bool write_ok;
    std::lock_guard<std::mutex>(this->topic_offsets_lock_);

    if(this->vme_device_ == NULL) return false;

    write_offset = this->header_.linked_list.topic_offsets_offset;

    for(auto & map_pair : this->topic_offsets){
      auto & topic_info = map_pair.first;
      vme_topic_offsets_t current_topic_offsets = map_pair.second;

      name = (topic_info.name + "\n");
      write_ok = fwrite64_atomic(this->vme_device_, (void*)name.c_str(), name.length(), write_offset);
      if(! write_ok) return false;
      write_offset += name.length();

      type = (topic_info.type + "\n");
      write_ok = fwrite64_atomic(this->vme_device_, (void*)type.c_str(), type.length(), write_offset);
      if(! write_ok) return false;
      write_offset += type.length();

      write_ok = fwrite64_atomic(this->vme_device_, &current_topic_offsets, sizeof(vme_topic_offsets_t), write_offset);
      if(! write_ok) return false;
      write_offset += sizeof(vme_topic_offsets_t);
    }

    return true;
  }

protected:
  virtual MessageQueue * create_message_queue(const SnapshotterTopicOptions & options) override
  {
    RCLCPP_DEBUG(this->get_logger(), "VMeSnapshotter::create_message_queue()");

    // Every topic's queue has to have access to the VMe namespace
    // shared with the VMeSnapshotter instance that they belong to
    vme_ns_state_t vme_ns_state = this->create_queue_vme_ns_state(NULL, NULL);

    // Returning an instance of VMeMessageQueue instead of a MessageQueue,
    // VMeMessageQueue extends MessageQueue
    return new VMeMessageQueue(vme_ns_state, options, this->get_logger());
  }

  vme_ns_state_t create_queue_vme_ns_state(vme_topic_offsets_t * p_offsets=NULL, std::mutex * p_lock=NULL)
  {
    return {
      p_offsets,
      this->vme_device_,
      &(this->header_.linked_list.next_write_offset),
      &(this->header_.linked_list.ns_usable_offset),
      &(this->header_.linked_list.topic_offsets_offset),
      p_lock,
      &(this->open_buffer_state)
    };
  }

  FILE * vme_device_;
  std::mutex header_lock_, topic_offsets_lock_, msg_storage_lock_;
  vme_circular_buffer_file_header_t header_;
  vme_topic_offsets_t open_buffer_state;
  key_t sem_key;
  int concurrent_write_semaphore;
  std::thread * concurrent_writer[ASYNCH_IO_QD];
  std::map<TopicDetails, vme_topic_offsets_t> topic_offsets;
  std::map<TopicDetails, vme_ns_state_t> topic_vme_state;
  std::string vme_ns_filename;
};

#endif

/// @file vme_message_queue.hpp
/// @author Gabriel Stewart
/// @copyright UPower (c) 2022

#ifndef VME_MESSAGE_QUEUE_H_INCLUDED
#define VME_MESSAGE_QUEUE_H_INCLUDED

#include <rosbag2_snapshot/vme_snapshot_msg.hpp>

// Define the buffer writing directionality
enum e_buffer_writing_direction{
  HEAD = e_entry_relationship_direction::NEXT,
  TAIL = e_entry_relationship_direction::PREVIOUS,
  DIR_NEWER = HEAD,
  DIR_OLDER = TAIL
};

typedef union{
    struct {
        off64_t head, tail;
    };
    off64_t a[NUM_DIRECTIONS] = {0, 0};
} vme_topic_offsets_t;

typedef struct {
  vme_topic_offsets_t * offsets;
  FILE * vme_ns_file;
  off64_t * next_write_offset;
  off64_t * ns_usable_offset;
  off64_t * topic_offsets_offset;
  std::mutex * lock;
  vme_topic_offsets_t * open_buffer_state;
} vme_ns_state_t;

class VMeMessageQueue : public MessageQueue
{
  friend VMeSnapshotter<VMeMessageQueue>;
  friend Snapshotter<VMeMessageQueue>;

public:
  explicit VMeMessageQueue(vme_ns_state_t & ns_state, const SnapshotterTopicOptions & options, const rclcpp::Logger & logger);
  ~VMeMessageQueue();

  vme_ns_state_t & vme_ns_state;

protected:
  void invalidate_message(entry_relationship_type_t rel_type = FILE_SEQUENTIAL, entry_relationship_direction_t rel_dir = NEXT);
  off64_t get_next_write_offset(VMeSnapshotMessage & msg);
  rclcpp::Time get_oldest_message_time() override;
  // Internal push whitch does not obtain lock
  void _push(SnapshotMessage const & msg) override;
  // Internal pop which does not obtain lock
  SnapshotMessage _pop() override;
  // Internal clear which does not obtain lock
  void _clear() override;
  std::deque<VMeSnapshotMessage> on_deck_queue;
};

#endif

/// @file vme_message_queue.cpp
/// @author Gabriel Stewart
/// @copyright UPower (c) 2022

#include <rosbag2_snapshot/vme_snapshotter.hpp>

VMeMessageQueue::VMeMessageQueue(vme_ns_state_t & ns_state, const SnapshotterTopicOptions & options, const rclcpp::Logger & logger) :
  MessageQueue(options, logger),
  vme_ns_state(ns_state)
{
}

VMeMessageQueue::~VMeMessageQueue()
{
}

MessageQueue * VMeMessageQueue::create_message_queue
  (Snapshotter<VMeMessageQueue> * p_node, const SnapshotterTopicOptions & options)
{
  auto p_vme_node = dynamic_cast<VMeSnapshotter<VMeMessageQueue> *>(p_node);
  assert(p_vme_node != NULL);
  return p_vme_node->create_message_queue(options);
}

rclcpp::Time VMeMessageQueue::get_oldest_message_time()
{
  if(this->vme_ns_state.offsets->a[HEAD] == 0){
    return MessageQueue::get_oldest_message_time();
  }

  VMeSnapshotMessage peek_msg(this->vme_ns_state.vme_ns_file, this->vme_ns_state.offsets->a[HEAD], this->vme_ns_state.lock);
  return peek_msg.time;
}

void VMeMessageQueue::invalidate_message(entry_relationship_type_t rel_type, entry_relationship_direction_t dir_newest)
{
  off64_t invalidated_msg_offset;

  // read the oldest message from the disk
  auto dir_oldest = OPPOSITE_DIRECTION(dir_newest);
  invalidated_msg_offset = this->vme_ns_state.open_buffer_state->a[dir_oldest];
  VMeSnapshotMessage invalidated_msg(this->vme_ns_state.vme_ns_file, invalidated_msg_offset);

  // Get the next valid message offset in the file
  auto related_msg_offset = invalidated_msg.offset.a[rel_type].a[dir_newest];

  // Update tail-if-forward/head-if-reverse to the next valid message
  this->vme_ns_state.offsets->a[dir_oldest] = related_msg_offset;
  if(related_msg_offset == 0){
    // corner case: the last message
    this->vme_ns_state.offsets->a[dir_newest] = 0;
    if(rel_type == FILE_SEQUENTIAL){
      this->vme_ns_state.open_buffer_state->a[dir_newest] = 0;
    }
    return;
  }

  // New head-or-tail is no longer related to the invalidated message.
  invalidated_msg._delete_from_storage(this->vme_ns_state.vme_ns_file);
}

off64_t VMeMessageQueue::get_next_write_offset(VMeSnapshotMessage & msg)
{
  off64_t next_write_offset, msg_write_offset;
  size_t msg_size;
  std::lock_guard<std::mutex> lock(*(this->vme_ns_state.lock));

  // Attach the new message to the current head message.
  msg.offset.in_file.prev = this->vme_ns_state.open_buffer_state->a[HEAD];

  msg_write_offset = (*this->vme_ns_state.next_write_offset);
  msg_size = msg.length(IN_STORAGE);
  next_write_offset = msg_write_offset + msg_size;

  if(next_write_offset > (*this->vme_ns_state.topic_offsets_offset)){
    // message would go past the end of the space for messages in the NS.
    while(this->vme_ns_state.offsets->a[HEAD] >= msg_write_offset){
      // Invalidate messages between where this message would have been written
      // and the end of the space for messages in the NS.
      this->invalidate_message(FILE_SEQUENTIAL, NEXT);
    }

    msg_write_offset = (*this->vme_ns_state.ns_usable_offset);
    next_write_offset = msg_write_offset + msg_size;
  }

  // The new message is now the head message.
  this->vme_ns_state.open_buffer_state->a[HEAD] = msg_write_offset;
  if(this->vme_ns_state.open_buffer_state->a[TAIL] == 0){
    this->vme_ns_state.open_buffer_state->a[TAIL] = msg_write_offset;
  }

  if(this->vme_ns_state.offsets->a[HEAD] != 0){
    while(this->vme_ns_state.offsets->a[HEAD] >= msg_write_offset &&
      this->vme_ns_state.offsets->a[HEAD] < next_write_offset){
      // Invalidate messages that will be overwritten by the new message
      this->invalidate_message(FILE_SEQUENTIAL, NEXT);
    }
  }

  (* this->vme_ns_state.next_write_offset) = next_write_offset;

  return msg_write_offset;
}

void VMeMessageQueue::_push(SnapshotMessage const & msg)
{
  off64_t next_write_offset;

  // Extend the message that was recieved so that it can be stored in the file
  VMeSnapshotMessage vme_msg(msg.msg, msg.time);

  // Assign the file-storage offset to the message
  next_write_offset = get_next_write_offset(vme_msg);
  vme_msg.set_file_offset(next_write_offset);

  // First message that is appended is held in on-deck status until
  // Second message arrives.
  if(this->on_deck_queue.size() == 0){
    this->on_deck_queue.push_back(vme_msg);
    return;
  }

  // Get the on-deck message
  auto vme_on_deck_msg = this->on_deck_queue.front();
  this->on_deck_queue.pop_front();
  this->on_deck_queue.push_back(vme_msg);

  // On-deck message is related to the new message, in storage
  vme_on_deck_msg.set_related_entry(&vme_msg, SAME_TOPIC, NEXT);

  // First message into the file?
  if(this->vme_ns_state.offsets->a[HEAD] == 0){
    this->vme_ns_state.offsets->a[HEAD] = vme_on_deck_msg.get_file_offset();
  }

  this->vme_ns_state.offsets->a[TAIL] = vme_on_deck_msg.get_file_offset();

  vme_on_deck_msg.write(this->vme_ns_state.vme_ns_file, this->vme_ns_state.lock);
}

SnapshotMessage VMeMessageQueue::_pop()
{
  // Corner case: no messages left in storage in file,
  // one message left in queue that was on-deck to write next.
  if(this->vme_ns_state.offsets->a[TAIL] == 0){
    // Get the on-deck message
    auto vme_on_deck_msg = this->on_deck_queue.front();
    this->on_deck_queue.pop_front();
    return vme_on_deck_msg;
  }

  std::lock_guard<std::mutex> lock(*(this->vme_ns_state.lock));

  // Read the tailing entry from storage:
  VMeSnapshotMessage msg(this->vme_ns_state.vme_ns_file, this->vme_ns_state.offsets->a[TAIL]);
  auto new_tail_offset = msg.offset.in_file.next;
  msg._delete_from_storage(this->vme_ns_state.vme_ns_file);

  this->vme_ns_state.offsets->a[TAIL] = new_tail_offset;

  return msg;
}

void VMeMessageQueue::_clear()
{
  std::lock_guard<std::mutex> lock(*(this->vme_ns_state.lock));

  while(this->vme_ns_state.offsets->a[TAIL] != 0){
    VMeSnapshotMessage storage_tail(this->vme_ns_state.vme_ns_file, this->vme_ns_state.offsets->a[TAIL]);
    this->vme_ns_state.offsets->a[TAIL] = storage_tail.offset.from_topic.next;
    storage_tail._delete_from_storage(this->vme_ns_state.vme_ns_file);
  }

  this->vme_ns_state.offsets->a[HEAD] = 0;
  this->vme_ns_state.offsets->a[TAIL] = 0;

  this->on_deck_queue.clear();
}

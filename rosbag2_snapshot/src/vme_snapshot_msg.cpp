/// @file vme_snapshot_msg.cpp
/// @author Gabriel Stewart
/// @copyright UPower (c) 2022

#include <rosbag2_snapshot/vme_snapshot_msg.hpp>
#include <aio.h>

bool foperation64_atomic(FILE * f_storage, void * buffer, size_t nbytes, off64_t abs_offset, int opcode, std::mutex * p_mutex){
  struct aiocb64 aio_cb, * aiocbp = &aio_cb;
  const struct aiocb64 * * aiocbp_list = (const struct aiocb64 * *)&aiocbp;
  std::lock_guard<std::mutex> * p_lock;

  if(p_mutex == NULL){
    p_lock = NULL;
  }
  else{
    p_lock = new std::lock_guard<std::mutex>(* p_mutex);
  }

  aio_cb.aio_fildes = fileno(f_storage);
  aio_cb.aio_buf = buffer;
  aio_cb.aio_nbytes = nbytes;
  aio_cb.aio_offset = abs_offset;
  aio_cb.aio_lio_opcode = opcode;
  aio_cb.aio_reqprio = 0; // same priority as the thread
  aio_cb.aio_sigevent.sigev_notify = SIGEV_NONE;

  switch(opcode){
    case LIO_WRITE:
      if(aio_write64(aiocbp) != 0){
        if(p_mutex != NULL){
          delete p_lock;
        }
        return false;
      }
      break;
    case LIO_READ:
      if(aio_read64(aiocbp) != 0){
        if(p_mutex != NULL){
          delete p_lock;
        }
        return false;
      }
      break;
    default:
      // invalid opcode
      if(p_mutex != NULL){
        delete p_lock;
      }
      return false;
  }

  bool complete_ok = aio_suspend64(aiocbp_list, 1, NULL) == 0;
  if(p_mutex != NULL){
    delete p_lock;
  }

  return complete_ok;
}

bool fread64_atomic(FILE * f_storage, void * buffer, size_t nbytes, off64_t abs_offset, std::mutex * p_mutex){
  return foperation64_atomic(f_storage, buffer, nbytes, abs_offset, LIO_READ, p_mutex);
}

bool fwrite64_atomic(FILE * f_storage, void * buffer, size_t nbytes, off64_t abs_offset, std::mutex * p_mutex){
  return foperation64_atomic(f_storage, buffer, nbytes, abs_offset, LIO_WRITE, p_mutex);
}

VMeSnapshotMessage::VMeSnapshotMessage(std::shared_ptr<const rclcpp::SerializedMessage> msg, rclcpp::Time time) :
  SnapshotMessage(msg, time)
{
  int idx;

  for(idx = 0; idx < NUM_FILE_OFFSETS; idx++){
    this->offset.offset_list[idx] = 0;
  }

  for(idx = 0; idx < NUM_LENGTH_TYPES; idx++){
    this->offset.length_list[idx] = this->length((entry_len_type_t)idx);
  }

  this->offset.time = time;

  this->_anull_all_related();
}

VMeSnapshotMessage::VMeSnapshotMessage(FILE * f_storage, off64_t read_offset, std::mutex * p_mutex) :
  SnapshotMessage(nullptr, rclcpp::Time())
{
  this->offset.offset = read_offset;

  this->_anull_all_related();

  auto p_msg = this->read(f_storage, p_mutex);

  SnapshotMessage(p_msg, this->offset.time);
}

VMeSnapshotMessage::~VMeSnapshotMessage()
{
}

/// Method for removing an entry from the linked list
void VMeSnapshotMessage::detach()
{
  VMeSnapshotMessage * p_relative;
  int rel_type, rel_dir, opposite_dir;

  // hand off all relational links:
  for(rel_type = 0; rel_type < NUM_RELATIONSHIP_TYPES; rel_type++){
    for(rel_dir = 0 ; rel_dir < NUM_DIRECTIONS; rel_dir++){
      opposite_dir = OPPOSITE_DIRECTION(rel_dir);
      if(this->related.a[rel_type].a[rel_dir] != NULL){
        p_relative = this->related.a[rel_type].a[rel_dir];
        if(p_relative == NULL) continue;

        p_relative->related.a[rel_type].a[opposite_dir] = this->related.a[rel_type].a[opposite_dir];
        p_relative->offset.a[rel_type].a[opposite_dir] = this->offset.a[rel_type].a[opposite_dir];
      }
    }
  }

  // abandon all relational links:
  this->_anull_all_related();
}

void VMeSnapshotMessage::_anull_all_related()
{
  for(int idx = 0; idx < NUM_RELATED_POINTERS; idx++){
    this->related.p[idx] = NULL;
  }
}

off64_t VMeSnapshotMessage::get_related_entry_offset(entry_relationship_type_t rel_type, entry_relationship_direction_t rel_dir)
{
  return this->offset.a[rel_type].a[rel_dir];
}

VMeSnapshotMessage * VMeSnapshotMessage::get_related_entry(entry_relationship_type_t rel_type, entry_relationship_direction_t rel_dir)
{
  return this->related.a[rel_type].a[rel_dir];
}

void VMeSnapshotMessage::set_related_entry(VMeSnapshotMessage * new_relative, entry_relationship_type_t rel_type, entry_relationship_direction_t rel_dir)
{
  auto opposite_dir = OPPOSITE_DIRECTION(rel_dir);

  this->related.a[rel_type].a[rel_dir] = new_relative;
  if(new_relative == NULL){
    this->offset.a[rel_type].a[rel_dir] = 0;
    return;
  }

  this->offset.a[rel_type].a[rel_dir] = new_relative->offset.offset;
  new_relative->related.a[rel_type].a[opposite_dir] = this;
  new_relative->offset.a[rel_type].a[opposite_dir] = this->offset.offset;
}

void VMeSnapshotMessage::set_file_offset(off64_t new_offset)
{
  this->offset.offset = new_offset;
}

off64_t VMeSnapshotMessage::get_file_offset()
{
  return this->offset.offset;
}

uint8_t * VMeSnapshotMessage::get_message_payload()
{
  auto ser_msg = this->msg->get_rcl_serialized_message();
  return ser_msg.buffer;
}

size_t VMeSnapshotMessage::length(entry_len_type_t len_type)
{
  auto ser_msg = this->msg->get_rcl_serialized_message();
  auto payload_len = ser_msg.buffer_length;

  switch(len_type){
    case PAYLOAD:
      return payload_len;
    case IN_STORAGE:
    default:
      return sizeof(vme_offsets_t) + payload_len;
  }
}

bool VMeSnapshotMessage::_delete_from_storage(FILE * f_storage, std::mutex * p_mutex){
  bool all_writes_ok = true;
  std::lock_guard<std::mutex> * p_lock;

  if(p_mutex == NULL){
    p_lock = NULL;
  }
  else{
    // Lock mutex for the reads and writes as one atomic operation, if a mutex was provided.
    p_lock = new std::lock_guard<std::mutex>(* p_mutex);
  }

  for(auto rel_type = 0; rel_type < NUM_RELATIONSHIP_TYPES; rel_type++){
    for(auto rel_dir = 0 ; rel_dir < NUM_DIRECTIONS; rel_dir++){
      auto opposite_dir = OPPOSITE_DIRECTION(rel_dir);
      if(this->offset.a[rel_type].a[rel_dir] != 0){
        VMeSnapshotMessage related_msg(f_storage, this->offset.a[rel_type].a[rel_dir]);
        related_msg.offset.a[rel_type].a[opposite_dir] = this->offset.a[rel_type].a[opposite_dir];
        if(! related_msg._write_offsets_only(f_storage)) all_writes_ok = false;
        this->offset.a[rel_type].a[rel_dir] = 0;
      }
    }
  }

  if(! this->_write_offsets_only(f_storage)) all_writes_ok = false;

  if(p_lock != NULL) delete p_lock;

  return all_writes_ok;
}

bool VMeSnapshotMessage::_write_offsets_only(FILE * f_storage, std::mutex * p_mutex){
  return fwrite64_atomic(
    f_storage,
    &this->offset,
    sizeof(vme_offsets_t),
    this->offset.offset,
    p_mutex
  );
}

bool VMeSnapshotMessage::write(FILE * f_storage, std::mutex * p_mutex){
  auto ser_msg = this->msg->get_rcl_serialized_message();
  auto payload = ser_msg.buffer;
  bool write_payload_ok;
  std::lock_guard<std::mutex> * p_lock;

  if(p_mutex == NULL){
    p_lock = NULL;
  }
  else{
    // Lock mutex for the write of the metadata and the payload
    // as one atomic operation, if a mutex was provided.
    p_lock = new std::lock_guard<std::mutex>(* p_mutex);
  }

  if(! this->_write_offsets_only(f_storage)){
    if(p_lock != NULL){
      delete p_lock;
    }
    return false;
  }

  write_payload_ok = fwrite64_atomic(
    f_storage,
    payload,
    this->offset.payload_size,
    this->offset.offset + sizeof(vme_offsets_t)
  );

  if(p_lock != NULL) delete p_lock;

  return write_payload_ok;
}

std::shared_ptr<const rclcpp::SerializedMessage> VMeSnapshotMessage::read(FILE * f_storage, std::mutex * p_mutex){
  off64_t read_offset = this->offset.offset;
  std::lock_guard<std::mutex> * p_lock;

  if(p_mutex == NULL){
    p_lock = NULL;
  }
  else{
    // Lock mutex for the read of the metadata and the payload
    // as one atomic operation, if a mutex was provided.
    p_lock = new std::lock_guard<std::mutex>(* p_mutex);
  }

  if(! fread64_atomic(f_storage, &this->offset, sizeof(vme_offsets_t), read_offset)){
    if(p_lock != NULL) delete p_lock;
    return nullptr;
  }

  if(this->offset.offset != read_offset){
    if(p_lock != NULL) delete p_lock;
    return nullptr;
  }

  auto ser_msg = std::make_shared<rclcpp::SerializedMessage>(this->offset.payload_size);
  auto & rcl_ser_msg = ser_msg->get_rcl_serialized_message();
  this->msg = ser_msg;

  if(! fread64_atomic(f_storage, rcl_ser_msg.buffer, this->offset.payload_size, read_offset + sizeof(vme_offsets_t))){
    if(p_lock != NULL) delete p_lock;
    return nullptr;
  }

  rcl_ser_msg.buffer_length = this->offset.payload_size;
  if(p_lock != NULL) delete p_lock;

  return this->msg;
}

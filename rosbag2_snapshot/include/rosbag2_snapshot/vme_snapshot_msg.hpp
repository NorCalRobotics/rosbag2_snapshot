/// @file vme_snapshot_msg.hpp
/// @author Gabriel Stewart
/// @copyright UPower (c) 2022

#ifndef VME_SNAPSHOT_MSG_H_INCLUDED
#define VME_SNAPSHOT_MSG_H_INCLUDED

#include <shared_mutex>
#include <rosbag2_snapshot/snapshotter.hpp>

using namespace rosbag2_snapshot;

// If >1 topics can access the message storage linked list simultaneously,
// there is a possibility of data corruption

/// Enumerates direction of relationships between nodes in the link list
typedef enum e_entry_relationship_direction{
    NEXT,
    PREVIOUS,
    NUM_DIRECTIONS
} entry_relationship_direction_t;

typedef union{
  struct {
    off64_t next;
    off64_t prev;
  };
  off64_t a[NUM_DIRECTIONS]; // "A" for array
} rel_offsets_t;

// Forward-delcaration of the classes allows them to reference each other,
// without catch-22's regarding order of appearance in the code
template <class TMessageQueue>
class VMeSnapshotter;
class VMeMessageQueue;
class VMeSnapshotMessage;

typedef union{
  struct {
    VMeSnapshotMessage * p_next;
    VMeSnapshotMessage * p_prev;
  };
  VMeSnapshotMessage * a[NUM_DIRECTIONS]; // "A" for array
} rel_pointers_t;

#define OPPOSITE_DIRECTION(DIR)    ((DIR == NEXT) ? PREVIOUS : NEXT)

/// Enumerates types of relationships between nodes in the link list
typedef enum e_entry_relationship_type{
    SAME_TOPIC,
    FILE_SEQUENTIAL,
    NUM_RELATIONSHIP_TYPES
} entry_relationship_type_t;

#define NUM_RELATED_POINTERS    (NUM_DIRECTIONS * NUM_RELATIONSHIP_TYPES)

typedef union{
  struct {
    rel_pointers_t from_topic;
    rel_pointers_t in_file;
  };
  rel_pointers_t a[NUM_RELATIONSHIP_TYPES]; // "A" for array
  VMeSnapshotMessage * p[NUM_RELATED_POINTERS];
} relatives_ram_pointers_t;

#define NUM_FILE_OFFSETS        (NUM_RELATED_POINTERS + 1)

/// Enumerates methods of calculating length of a node
typedef enum e_entry_len_type{
    PAYLOAD,
    IN_STORAGE,
    NUM_LENGTH_TYPES
} entry_len_type_t;


typedef struct{
  union{
    struct {
      // This metadata is stored on drive as metadata for each saved message.
      rel_offsets_t from_topic;
      rel_offsets_t in_file;
      off64_t offset;
      size_t payload_size;
      size_t storage_size;
    };
    rel_offsets_t a[NUM_RELATIONSHIP_TYPES]; // "A" for array
    struct {
      off64_t offset_list[NUM_FILE_OFFSETS];
      size_t length_list[NUM_LENGTH_TYPES];
    };
  };
  rclcpp::Time time = rclcpp::Time();
} vme_offsets_t;

/// SnapshotMessage class, extended with file offsets for prev/next
/// messages stored on VMe drive.
class VMeSnapshotMessage : SnapshotMessage
{
public:
  VMeSnapshotMessage(std::shared_ptr<const rclcpp::SerializedMessage> msg, rclcpp::Time time);
  VMeSnapshotMessage(FILE * f_storage, off64_t read_offset, std::mutex * p_mutex=NULL);
  ~VMeSnapshotMessage();

  friend VMeMessageQueue;
  friend VMeSnapshotter<VMeMessageQueue>;

  bool write(FILE * f_storage, std::mutex * p_mutex=NULL);
  std::shared_ptr<const rclcpp::SerializedMessage> read(FILE * f_storage, std::mutex * p_mutex=NULL);

  void detach();
  off64_t get_related_entry_offset(entry_relationship_type_t rel_type = SAME_TOPIC, entry_relationship_direction_t rel_dir = NEXT);
  VMeSnapshotMessage * get_related_entry(entry_relationship_type_t rel_type = SAME_TOPIC, entry_relationship_direction_t rel_dir = NEXT);
  void set_related_entry(VMeSnapshotMessage * new_relative, entry_relationship_type_t rel_type = SAME_TOPIC, entry_relationship_direction_t rel_dir = NEXT);
  void set_file_offset(off64_t new_offset);
  off64_t get_file_offset();
  size_t length(entry_len_type_t len_type = IN_STORAGE);
  uint8_t * get_message_payload();

protected:
  bool _delete_from_storage(FILE * f_storage, std::mutex * p_mutex=NULL);
  bool _write_offsets_only(FILE * f_storage, std::mutex * p_mutex=NULL);
  void _anull_all_related();

  vme_offsets_t offset;
  relatives_ram_pointers_t related;
};

bool fread64_atomic(FILE * f_storage, void * buffer, size_t nbytes, off64_t abs_offset, std::mutex * p_mutex=NULL);
bool fwrite64_atomic(FILE * f_storage, void * buffer, size_t nbytes, off64_t abs_offset, std::mutex * p_mutex=NULL);

#endif

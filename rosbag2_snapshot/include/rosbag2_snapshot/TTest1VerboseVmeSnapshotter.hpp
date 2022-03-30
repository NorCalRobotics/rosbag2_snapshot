/// @file TTest1VerboseVmeSnapshotter.hpp
/// @author Gabriel Stewart
/// @copyright UPower (c) 2022

#ifndef TEMPLATE_TEST1_VERBOSE_VME_SNAPSHOTTER_H_INCLUDED
#define TEMPLATE_TEST1_VERBOSE_VME_SNAPSHOTTER_H_INCLUDED

#include <rosbag2_snapshot/vme_snapshotter.hpp>

template <class TMessageQueue>
class Test1VerboseVMeSnapshotter : public VMeSnapshotter<TMessageQueue> {
  friend TMessageQueue;

public:
  explicit Test1VerboseVMeSnapshotter(const rclcpp::NodeOptions &options, std::experimental::filesystem::v1::__cxx11::path vme_device) :
    VMeSnapshotter<TMessageQueue>(options, vme_device)
  {
    auto v_overrides = options.parameter_overrides();
  
    for(size_t override_idx = 0; override_idx < v_overrides.size(); override_idx++){
      auto & param = v_overrides[override_idx];
      if(param.get_type() == rclcpp::PARAMETER_STRING){
        RCLCPP_INFO(this->get_logger(),
          "Constructing Test1VerboseVMeSnapshotter instance with parameter \"%s\" = \"%s\".",
          param.get_name().c_str(), param.as_string().c_str()
        );
        continue;
      }
      else if(param.get_type() == rclcpp::PARAMETER_DOUBLE){
        RCLCPP_INFO(this->get_logger(),
          "Constructing Test1VerboseVMeSnapshotter instance with parameter \"%s\" = %lf.",
          param.get_name().c_str(), param.as_double()
        );
        continue;
      }
      else if(param.get_type() != rclcpp::PARAMETER_STRING_ARRAY) continue;
  
      auto topic_names = param.as_string_array();
      std::stringstream vector_rep_builder;
      std::string delim = "[";
      for(size_t idx = 0; idx < topic_names.size(); idx++){
        vector_rep_builder << delim << topic_names[idx];
        delim = ", ";
      }
      std::string ending = (delim.compare("[") == 0) ? "[]" : "]";
      vector_rep_builder << ending;
      auto vector_rep = vector_rep_builder.str();
      RCLCPP_INFO(this->get_logger(),
        "Constructing Test1VerboseVMeSnapshotter instance with parameter \"%s\" = %s.",
        param.get_name().c_str(), vector_rep.c_str()
      );
    }
  }

  ~Test1VerboseVMeSnapshotter(){}

  void init_empty_buffer_header() override
  {
    RCLCPP_INFO(this->get_logger(), "Test1VerboseVMeSnapshotter::init_empty_buffer_header().");
    this->VMeSnapshotter<TMessageQueue>::init_empty_buffer_header();
  }

  bool read_header() override
  {
    RCLCPP_INFO(this->get_logger(), "Test1VerboseVMeSnapshotter::read_header().");
    return this->VMeSnapshotter<TMessageQueue>::read_header();
  }

  bool write_header() override
  {
    RCLCPP_INFO(this->get_logger(), "Test1VerboseVMeSnapshotter::write_header().");
    return this->VMeSnapshotter<TMessageQueue>::write_header();
  }

  bool read_topic_offsets() override
  {
    RCLCPP_INFO(this->get_logger(), "Test1VerboseVMeSnapshotter::read_topic_offsets().");
    return this->VMeSnapshotter<TMessageQueue>::read_topic_offsets();
  }

  bool write_topic_offsets() override
  {
    RCLCPP_INFO(this->get_logger(), "Test1VerboseVMeSnapshotter::write_topic_offsets().");
    return this->VMeSnapshotter<TMessageQueue>::write_topic_offsets();
  }

protected:
  MessageQueue * create_message_queue(const SnapshotterTopicOptions & options) override
  {
    RCLCPP_DEBUG(this->get_logger(), "Test1VerboseVMeSnapshotter::create_message_queue()");
    // Every topic's queue has to have access to the VMe namespace
    // shared with the VMeSnapshotter instance that they belong to
    vme_ns_state_t vme_ns_state = this->create_queue_vme_ns_state(NULL, NULL);
  
    // Returning an instance of Test1VerboseVMeMessageQueue instead of a MessageQueue,
    // Test1VerboseVMeMessageQueue extends VMeMessageQueue;
    // VMeMessageQueue extends MessageQueue
    return new Test1VerboseVMeMessageQueue(this, vme_ns_state, options, this->get_logger());
  }

  void topicCb(
    std::shared_ptr<const rclcpp::SerializedMessage> msg,
    std::shared_ptr<TMessageQueue> queue) override
  {
    auto logger = this->get_logger();
    auto payload = msg->get_rcl_serialized_message().buffer;
  
    RCLCPP_INFO(logger, "Received message \"%s\".", payload);
  
    this->Snapshotter<TMessageQueue>::topicCb(msg, queue);
  }

  void parseOptionsFromParams() override
  {
    this->Snapshotter<TMessageQueue>::parseOptionsFromParams();
  
    for(auto & pair : this->options_.topics_){
      RCLCPP_INFO(this->get_logger(), "After parseOptionsFromParams(): topic name = \"%s\"", pair.first.name.c_str());
    }
  }
};

#endif

/// @file TSnapshotter.hpp
// Copyright (c) 2018-2022, Open Source Robotics Foundation, Inc., GAIA Platform, Inc., UPower Robotics USA, All rights reserved.  // NOLINT
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//    * Redistributions of source code must retain the above copyright
//      notice, this list of conditions and the following disclaimer.
//
//    * Redistributions in binary form must reproduce the above copyright
//      notice, this list of conditions and the following disclaimer in the
//      documentation and/or other materials provided with the distribution.
//
//    * Neither the name of the {copyright_holder} nor the names of its
//      contributors may be used to endorse or promote products derived from
//      this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

#ifndef ROSBAG2_SNAPSHOT__TEMPLATE_SNAPSHOTTER_HPP_
#define ROSBAG2_SNAPSHOT__TEMPLATE_SNAPSHOTTER_HPP_

#include <rclcpp/rclcpp.hpp>
#include <rclcpp/time.hpp>
#include <rosbag2_snapshot_msgs/msg/topic_details.hpp>
#include <rosbag2_snapshot_msgs/srv/trigger_snapshot.hpp>
#include <std_srvs/srv/set_bool.hpp>
#include <rosbag2_cpp/writer.hpp>

#include <chrono>
#include <deque>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <shared_mutex>

using namespace std::chrono_literals;  // NOLINT
using std::placeholders::_1;
using std::placeholders::_2;
using std::placeholders::_3;
using std::string;
using rosbag2_snapshot_msgs::srv::TriggerSnapshot;
using std_srvs::srv::SetBool;

static constexpr uint32_t MB_TO_B = 1e6;

// Snapshotter node. Maintains a circular buffer of the most recent messages
// from configured topics while enforcing limits on memory and duration.
// The node can be triggered to write some or all of these buffers to a bag
// file via a service call. Useful in live testing scenerios where interesting
// data may be produced before a user has the oppurtunity to "rosbag record" the data.
template <class TMessageQueue> class Snapshotter : public rclcpp::Node
{
  static_assert(std::is_base_of<MessageQueue, TMessageQueue>::value, "TMessageQueue must inherit from MessageQueue");
  friend TMessageQueue;

public:
  explicit Snapshotter(const rclcpp::NodeOptions & options)
  : rclcpp::Node("snapshotter", options),
    recording_(true),
    writing_(false)
  {
    parseOptionsFromParams();

    // Create the queue for each topic and set up the subscriber to add to it on new messages
    for (auto & pair : options_.topics_) {
      string topic{pair.first.name}, type{pair.first.type};
      fixTopicOptions(pair.second);
      auto p_new_base_queue = TMessageQueue::create_message_queue(this, pair.second);
      auto p_new_queue = dynamic_cast<TMessageQueue *>(p_new_base_queue);
      assert(p_new_queue != NULL);
      msg_queue_t queue;
      queue.reset(p_new_queue);

      TopicDetails details{};
      details.name = topic;
      details.type = type;


      auto res = buffers_.emplace(details, queue);

      assert(res.second);

      subscribe(details, queue);
    }

    // Now that subscriptions are setup, setup service servers for writing and pausing
    trigger_snapshot_server_ = create_service<TriggerSnapshot>(
      "trigger_snapshot", std::bind(&Snapshotter::triggerSnapshotCb, this, _1, _2, _3));
    enable_server_ = create_service<SetBool>(
      "enable_snapshot", std::bind(&Snapshotter::enableCb, this, _1, _2, _3));

    // Start timer to poll for topics
    if (options_.all_topics_) {
      poll_topic_timer_ =
        create_wall_timer(
        std::chrono::duration(1s),
        std::bind(&Snapshotter::pollTopics, this));
    }
  }

  ~Snapshotter()
  {
    for (auto & buffer : buffers_) {
      buffer.second->sub_.reset();
    }
  }

protected:
  typedef std::shared_ptr<TMessageQueue> msg_queue_t;
  typedef std::map<TopicDetails, msg_queue_t> buffers_t;
  virtual MessageQueue * create_message_queue(const SnapshotterTopicOptions & options)
  {
    RCLCPP_DEBUG(this->get_logger(), "Snapshotter::create_message_queue()");
    return new MessageQueue(options, this->get_logger());
  }

  // Convert parameter values into a SnapshotterOptions object
  virtual void parseOptionsFromParams()
  {
    std::vector<std::string> topics{};

    try {
      options_.default_duration_limit_ = rclcpp::Duration::from_seconds(
        declare_parameter<double>("default_duration_limit", -1.0));
    } catch (const rclcpp::ParameterTypeException & ex) {
      RCLCPP_ERROR(get_logger(), "default_duration_limit is of incorrect type.");
      throw ex;
    }

    try {
      options_.default_memory_limit_ =
        declare_parameter<double>("default_memory_limit", -1.0);
    } catch (const rclcpp::ParameterTypeException & ex) {
      RCLCPP_ERROR(get_logger(), "default_memory_limit is of incorrect type.");
      throw ex;
    }

    try {
      options_.system_wide_memory_limit_ =
        declare_parameter<double>("system_wide_memory_limit", -1.0);
    } catch (const rclcpp::ParameterTypeException & ex) {
      RCLCPP_ERROR(get_logger(), "system_wide_memory_limit is of incorrect type.");
      throw ex;
    }

    // Convert memory limit in MB to B
    if (options_.default_memory_limit_ != -1.0) {
      options_.default_memory_limit_ *= MB_TO_B;
    }

    try {
      topics = declare_parameter<std::vector<std::string>>(
        "topics", std::vector<std::string>{});
    } catch (const rclcpp::ParameterTypeException & ex) {
      if (std::string{ex.what()}.find("not set") == std::string::npos) {
        RCLCPP_ERROR(get_logger(), "topics must be an array of strings.");
        throw ex;
      }
    }

    if (topics.size() > 0) {
      options_.all_topics_ = false;

      for (const auto & topic : topics) {
        std::string prefix = "topic_details." + topic;
        std::string topic_type{};
        SnapshotterTopicOptions opts{};

        try {
          topic_type = declare_parameter<std::string>(prefix + ".type");
        } catch (const rclcpp::ParameterTypeException & ex) {
          if (std::string{ex.what()}.find("not set") == std::string::npos) {
            RCLCPP_ERROR(get_logger(), "Topic type must be a string.");
          } else {
            RCLCPP_ERROR(get_logger(), "Topic %s is missing a type.", topic.c_str());
          }

          throw ex;
        }

        try {
          opts.duration_limit_ = rclcpp::Duration::from_seconds(
            declare_parameter<double>(prefix + ".duration")
          );
        } catch (const rclcpp::ParameterTypeException & ex) {
          if (std::string{ex.what()}.find("not set") == std::string::npos) {
            RCLCPP_ERROR(
              get_logger(), "Duration limit for topic %s must be a double.", topic.c_str());
            throw ex;
          }
        }

        try {
          opts.memory_limit_ = declare_parameter<double>(prefix + ".memory");
        } catch (const rclcpp::ParameterTypeException & ex) {
          if (std::string{ex.what()}.find("not set") == std::string::npos) {
            RCLCPP_ERROR(
              get_logger(), "Memory limit for topic %s is of the wrong type.", topic.c_str());
            throw ex;
          }
        }

        TopicDetails dets{};
        dets.name = topic;
        dets.type = topic_type;

        options_.topics_.insert(
          SnapshotterOptions::topics_t::value_type(dets, opts));
      }
    } else {
      options_.all_topics_ = true;
      RCLCPP_INFO(get_logger(), "No topics list provided. Logging all topics.");
      RCLCPP_WARN(get_logger(), "Logging all topics is very memory-intensive.");
    }
  }

  // Called on new message from any configured topic. Adds to queue for that topic
  virtual void topicCb(
    std::shared_ptr<const rclcpp::SerializedMessage> msg,
    msg_queue_t queue)
  {
    // If recording is paused (or writing), exit
    {
      std::shared_lock<std::shared_mutex> lock(state_lock_);
      if (!recording_) {
        return;
      }
    }

    // Pack message and metadata into SnapshotMessage holder
    SnapshotMessage out(msg, now());
    queue->push(out);
  }

  buffers_t & get_message_queue_map(){ return this->buffers_; }

private:
  // Replace individual topic limits with node defaults if they are
  // flagged for it (see SnapshotterTopicOptions)
  void fixTopicOptions(SnapshotterTopicOptions & options)
  {
    if (options.duration_limit_ == SnapshotterTopicOptions::INHERIT_DURATION_LIMIT) {
      options.duration_limit_ = options_.default_duration_limit_;
    }
    if (options.memory_limit_ == SnapshotterTopicOptions::INHERIT_MEMORY_LIMIT) {
      options.memory_limit_ = options_.default_memory_limit_;
    }
  }

  // If file is "prefix" mode (doesn't end in .bag), append current datetime and .bag to end
  bool postfixFilename(std::string & file)
  {
    size_t ind = file.rfind(".bag");
    // If requested ends in .bag, this is literal name do not append date
    if (ind != string::npos && ind == file.size() - 4) {
      return true;
    }
    // Otherwise treat as prefix and append datetime and extension
    file += timeAsStr() + ".bag";
    return true;
  }

  /// Return current local datetime as a string such as 2018-05-22-14-28-51.
  // Used to generate bag filenames
  std::string timeAsStr()
  {
    std::stringstream msg;
    const auto now = std::chrono::system_clock::now();
    const auto now_in_t = std::chrono::system_clock::to_time_t(now);
    msg << std::put_time(std::localtime(&now_in_t), "%Y-%m-%d-%H-%M-%S");
    return msg.str();
  }

  // Clear the internal buffers of all topics. Used when resuming after a pause to avoid time gaps
  void clear()
  {
    for (const auto & pair : buffers_) {
      pair.second->clear();
    }
  }

  // Subscribe to one of the topics, setting up the callback to add to the respective queue
  void subscribe(
    const TopicDetails & topic_details,
    std::shared_ptr<TMessageQueue> queue)
  {
    RCLCPP_INFO(get_logger(), "Subscribing to %s", topic_details.name.c_str());

    auto opts = rclcpp::SubscriptionOptions{};
    opts.topic_stats_options.state = rclcpp::TopicStatisticsState::Enable;
    opts.topic_stats_options.publish_topic = topic_details.name + "/statistics";

    auto sub_cb_f = [this, queue](std::shared_ptr<const rclcpp::SerializedMessage> msg){
      this->topicCb(msg, queue);
    };

    auto sub = create_generic_subscription(
      topic_details.name,
      topic_details.type,
      rclcpp::QoS{10},
      sub_cb_f,
      opts
    );

    queue->setSubscriber(sub);
  }

  // Service callback, write all of part of the internal buffers to a bag file
  // according to request parameters
  void triggerSnapshotCb(
    const std::shared_ptr<rmw_request_id_t> request_header,
    const rosbag2_snapshot_msgs::srv::TriggerSnapshot::Request::SharedPtr req,
    rosbag2_snapshot_msgs::srv::TriggerSnapshot::Response::SharedPtr res
  )
  {
    (void)request_header;

    if (req->filename.empty() || !postfixFilename(req->filename)) {
      res->success = false;
      res->message = "Invalid filename";
      return;
    }

    // Store if we were recording prior to write to restore this state after write
    bool recording_prior{true};

    {
      std::shared_lock<std::shared_mutex> read_lock(state_lock_);
      recording_prior = recording_;
      if (writing_) {
        res->success = false;
        res->message = "Already writing";
        return;
      }
    }

    {
      std::unique_lock<std::shared_mutex> write_lock(state_lock_);
      if (recording_prior) {
        pause();
      }
      writing_ = true;
    }

    // Ensure that state is updated when function exits, regardlesss of branch path / exception events
    RCLCPP_SCOPE_EXIT(
      // Clear buffers beacuase time gaps (skipped messages) may have occured while paused
      std::unique_lock<std::shared_mutex> write_lock(state_lock_);
      // Turn off writing flag and return recording to its state before writing
      writing_ = false;
      if (recording_prior) {
        this->resume();
      }
    );

    rosbag2_cpp::Writer bag_writer{};

    try {
      bag_writer.open(req->filename);
    } catch (const std::exception & ex) {
      res->success = false;
      res->message = "Unable to open file for writing.";
      return;
    }

    // Write each selected topic's queue to bag file
    if (req->topics.size() && req->topics.at(0).name.size() && req->topics.at(0).type.size()) {
      for (auto & topic : req->topics) {
        TopicDetails details{topic.name, topic.type};
        // Find the message queue for this topic if it exsists
        auto found = buffers_.find(details);

        if (found == buffers_.end()) {
          RCLCPP_WARN(
            get_logger(), "Requested topic %s is not subscribed, skipping.", topic.name.c_str());
          continue;
        }

        TMessageQueue & message_queue = *(found->second);

        if (!writeTopic(bag_writer, message_queue, details, req, res)) {
          res->success = false;
          res->message = "Failed to write topic " + topic.type + " to bag file.";
          return;
        }
      }
    } else {  // If topic list empty, record all buffered topics
      for (const auto & pair : buffers_) {
        TMessageQueue & message_queue = *(pair.second);
        if (!writeTopic(bag_writer, message_queue, pair.first, req, res)) {
          res->success = false;
          res->message = "Failed to write topic " + pair.first.name + " to bag file.";
          return;
        }
      }
    }

    /*
    // If no topics were subscribed/valid/contained data, this is considered a non-success
    if (!bag.isOpen()) {
      res->success = false;
      res->message = res->NO_DATA_MESSAGE;
      return;
    }
    */

    res->success = true;
  }

  // Service callback, enable or disable recording (storing new messages into queue).
  // Used to pause before writing
  void enableCb(
    const std::shared_ptr<rmw_request_id_t> request_header,
    const std_srvs::srv::SetBool::Request::SharedPtr req,
    std_srvs::srv::SetBool_Response::SharedPtr res
  )
  {
    (void)request_header;

    {
      std::shared_lock<std::shared_mutex> read_lock(state_lock_);
      // Cannot enable while writing
      if (req->data && writing_) {
        res->success = false;
        res->message = "cannot enable recording while writing.";
        return;
      }
    }

    // Obtain write lock and update state if requested state is different from current
    if (req->data && !recording_) {
      std::unique_lock<std::shared_mutex> write_lock(state_lock_);
      resume();
    } else if (!req->data && recording_) {
      std::unique_lock<std::shared_mutex> write_lock(state_lock_);
      pause();
    }

    res->success = true;
  }

  // Set recording_ to false and do nessesary cleaning, CALLER MUST OBTAIN LOCK
  void pause()
  {
    RCLCPP_INFO(get_logger(), "Buffering paused");
    recording_ = false;
  }

  // Set recording_ to true and do nesessary cleaning, CALLER MUST OBTAIN LOCK
  void resume()
  {
    clear();
    recording_ = true;
    RCLCPP_INFO(get_logger(), "Buffering resumed and old data cleared.");
  }

  // Poll master for new topics
  void pollTopics()
  {
    const auto topic_names_and_types = get_topic_names_and_types();

    for (const auto & name_type : topic_names_and_types) {
      if (name_type.second.size() < 1) {
        RCLCPP_ERROR(get_logger(), "Subscribed topic has no associated type.");
        return;
      }

      if (name_type.second.size() > 1) {
        RCLCPP_ERROR(get_logger(), "Subscribed topic has more than one associated type.");
        return;
      }

      TopicDetails details{};
      details.name = name_type.first;
      details.type = name_type.second[0];

      if (options_.addTopic(details)) {
        SnapshotterTopicOptions topic_options;
        fixTopicOptions(topic_options);
        auto p_new_base_queue = TMessageQueue::create_message_queue(this, topic_options);
        auto p_new_queue = dynamic_cast<TMessageQueue *>(p_new_base_queue);
        assert(p_new_queue != NULL);
        msg_queue_t queue;
        queue.reset(p_new_queue);

        auto res = buffers_.emplace(details, queue);

        assert(res.second);
        subscribe(details, queue);
      }
    }
  }

  // Write the parts of message_queue within the time constraints of req to the queue
  // If returns false, there was an error opening/writing the bag and an error message
  // was written to res.message
  bool writeTopic(
    rosbag2_cpp::Writer & bag_writer, TMessageQueue & message_queue,
    const TopicDetails & topic_details,
    const rosbag2_snapshot_msgs::srv::TriggerSnapshot::Request::SharedPtr & req,
    const rosbag2_snapshot_msgs::srv::TriggerSnapshot::Response::SharedPtr & res)
  {
    // acquire lock for this queue
    std::lock_guard l(message_queue.lock);

    MessageQueue::range_t range = message_queue.rangeFromTimes(req->start_time, req->stop_time);

    rosbag2_storage::TopicMetadata tm;
    tm.name = topic_details.name;
    tm.type = topic_details.type;
    tm.serialization_format = "cdr";

    bag_writer.create_topic(tm);

    for (auto msg_it = range.first; msg_it != range.second; ++msg_it) {
      // Create BAG message
      auto bag_message = std::make_shared<rosbag2_storage::SerializedBagMessage>();
      auto ret = rcutils_system_time_now(&bag_message->time_stamp);
      if (ret != RCL_RET_OK) {
        RCLCPP_ERROR(get_logger(), "Failed to assign time to rosbag message.");
        return false;
      }

      bag_message->topic_name = tm.name;
      bag_message->time_stamp = msg_it->time.nanoseconds();
      bag_message->serialized_data = std::make_shared<rcutils_uint8_array_t>(
        msg_it->msg->get_rcl_serialized_message()
      );

      bag_writer.write(bag_message);
    }

    return true;
  }

protected:
  SnapshotterOptions options_;

private:
  // Subscribe queue size for each topic
  static const int QUEUE_SIZE = 10;
  buffers_t buffers_;
  // Locks recording_ and writing_ states.
  std::shared_mutex state_lock_;
  // True if new messages are being written to the internal buffer
  bool recording_;
  // True if currently writing buffers to a bag file
  bool writing_;
  rclcpp::Service<rosbag2_snapshot_msgs::srv::TriggerSnapshot>::SharedPtr
    trigger_snapshot_server_;
  rclcpp::Service<std_srvs::srv::SetBool>::SharedPtr enable_server_;
  rclcpp::TimerBase::SharedPtr poll_topic_timer_;
};

#endif // ROSBAG2_SNAPSHOT__TEMPLATE_SNAPSHOTTER_HPP_

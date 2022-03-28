/// @file vme_snapshotter.cpp
/// @author Gabriel Stewart
/// @copyright UPower (c) 2022

#include <rosbag2_snapshot/vme_snapshotter.hpp>
#include <rosbag2_snapshot/payload_block_math.h>

template class Snapshotter<VMeMessageQueue>;
template class VMeSnapshotter<VMeMessageQueue>;

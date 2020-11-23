//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include "common/logger.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  size = 0;
  capacity = num_pages;
  LRUQueue = new QueueLinkedList();
}

LRUReplacer::~LRUReplacer() { delete LRUQueue; }

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  mtx.lock();

  if (LRUQueue->isEmpty()) {
    mtx.unlock();
    return false;
  }

  *frame_id = LRUQueue->pop();
  size--;

  mtx.unlock();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  mtx.lock();

  if (!LRUQueue->contains(frame_id)) {
    mtx.unlock();
    return;
  }

  LRUQueue->remove(frame_id);
  size--;

  mtx.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  mtx.lock();

  if (LRUQueue->contains(frame_id)) {
    mtx.unlock();
    return;
  }

  LRUQueue->add(frame_id);
  size++;

  mtx.unlock();
}

size_t LRUReplacer::Size() { return size; }
}  // namespace bustub

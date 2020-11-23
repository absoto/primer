//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <utility>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"
#include "common/logger.h"

namespace bustub {
struct node {
  frame_id_t data;
  node *next;
  node *prev;
};

/* QueueLinkedList implements a queue that with traversal capability */
struct QueueLinkedList {
 public:
  QueueLinkedList() {
    head = new node;
    tail = new node;
    head->prev = nullptr;
    head->next = tail;
    tail->prev = head;
    tail->next = nullptr;
    size = 0;
  }

  ~QueueLinkedList() {
    while (!isEmpty()) {
      pop();
    }

    head->next = nullptr;
    tail->next = nullptr;

    delete head;
    delete tail;
  }

  bool isEmpty() { return size == 0; }

  void add(frame_id_t frame_id) {
    node *tmp = new node;
    tmp->data = frame_id;

    tmp->prev = head;
    tmp->next = head->next;
    tmp->next->prev = tmp;
    head->next = tmp;

    size++;

    std::pair<frame_id_t, node *> elem(frame_id, tmp);
    map_.insert(elem);
  }

  // requires queue is not empty
  frame_id_t pop() {
    node *tmp = tail->prev;
    frame_id_t frame_id = tmp->data;

    tmp->prev->next = tail;
    tail->prev = tmp->prev;

    tmp->prev = nullptr;
    tmp->next = nullptr;

    map_.erase(frame_id);
    delete tmp;

    size--;

    return frame_id;
  }

  bool contains(frame_id_t frame_id) {
    std::unordered_map<frame_id_t, node *>::const_iterator index = map_.find(frame_id);
    bool result = true;

    if (index == map_.end()) {
      result = false;
    }

    return result;
  }

  void remove(frame_id_t frame_id) {
    std::unordered_map<frame_id_t, node *>::const_iterator index = map_.find(frame_id);

    if (index != map_.end()) {
      frame_id_t frame_id = index->first;
      node *tmp = index->second;

      tmp->prev->next = tmp->next;
      tmp->next->prev = tmp->prev;

      tmp->prev = nullptr;
      tmp->next = nullptr;

      size--;

      map_.erase(frame_id);
      delete tmp;
    }
  }

 private:
  node *head;
  node *tail;
  size_t size;
  std::unordered_map<frame_id_t, node *> map_;
};

/**
 * LRUReplacer implements the lru replacement policy, which approximates the Least Recently Used policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 private:
  // TODO(student): implement me!
  size_t size;
  size_t capacity;
  QueueLinkedList *LRUQueue;
  mutable std::mutex mtx;
};

}  // namespace bustub

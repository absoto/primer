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
#include <vector>
#include <unordered_map>

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
class QueueLinkedList {
 public:
  QueueLinkedList() {
    head = nullptr;
    tail = nullptr;
  }

  bool isEmpty() { return head == nullptr; }

  void add(frame_id_t frame_id) {
    node *tmp = new node;
    tmp->data = frame_id;
    tmp->next = nullptr;
    tmp->prev = nullptr;

    if (head == nullptr) {
      head = tmp;
      tail = tmp;
    } else {
      tmp->next = head;
      head->prev = tmp;
      head = tmp;
    }

    map_.insert(std::pair<frame_id_t, node*>(frame_id,tmp));
  }

  // requires queue is not empty
  frame_id_t pop() {
    node *tmp = tail;
    frame_id_t frame_id = tmp->data;

    if (head != tail) {
      tail = tail->prev;
      tail->next = nullptr;
    } else {
      head = nullptr;
      tail = nullptr;
    }

    delete tmp;
    map_.erase(frame_id);

    return frame_id;
  }

  bool contains(frame_id_t frame_id) {
    std::unordered_map<frame_id_t, node*>::const_iterator index = map_.find (frame_id);

    if(index == map_.end()) {
      return false;
    }

    return true;
  }

  void remove(frame_id_t frame_id) {
    std::unordered_map<frame_id_t, node*>::const_iterator index = map_.find (frame_id);

    if(index == map_.end()) {
      return;
    } else {
      frame_id_t frame_id = index->first;
      node *tmp = index->second;

      if (tmp == tail) {
        pop();
        return;
      }

      if (tmp->next != nullptr) {
        tmp->next->prev = tmp->prev;
      }

      if (tmp->prev != nullptr) {
        tmp->prev->next = tmp->next;
      }

      if (tmp == head) {
        head = head->next;

        if (head != nullptr) {
          head->prev = nullptr;
        }
      }

      delete tmp; 
      map_.erase(frame_id);  
    }
  }

 private:
  node *head;
  node *tail;
  std::unordered_map<frame_id_t, node*> map_;
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
  QueueLinkedList LRUQueue;
  mutable std::mutex mtx; 
};

}  // namespace bustub

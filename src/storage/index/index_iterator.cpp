/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(LeafPage *page, int index, const KeyComparator &comparator,
                                  BufferPoolManager *buffer_pool_manager)
    : curr_page_(page), index_(index), buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (!isEnd()) {
    buffer_pool_manager_->UnpinPage(curr_page_->BPlusTreePage::GetPageId(), false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { return curr_page_ == nullptr; }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() { return curr_page_->GetItem(index_); }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  int curr_size = curr_page_->BPlusTreePage::GetSize();
  index_++;

  if (index_ >= curr_size) {
    page_id_t next_page_id = curr_page_->GetNextPageId();
    LeafPage *next_page;

    if (next_page_id == INVALID_PAGE_ID) {
      next_page = nullptr;
    } else {
      Page *page = buffer_pool_manager_->FetchPage(next_page_id);
      next_page = reinterpret_cast<LeafPage *>(page->GetData());
    }

    buffer_pool_manager_->UnpinPage(curr_page_->BPlusTreePage::GetPageId(), false);
    curr_page_ = next_page;
    index_ = 0;
  }

  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) {
  IndexIterator itr_1 = *this;
  IndexIterator itr_2 = itr;

  if (isEnd()) {
    return itr_2.isEnd();
  }

  if (itr_2.isEnd()) {
    return false;
  }

  MappingType m_1 = *itr_1;
  MappingType m_2 = *itr_2;
  KeyType key_1 = m_1.first;
  KeyType key_2 = m_2.first;

  return comparator_(key_1, key_2) == 0;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) {
  IndexIterator itr_1 = *this;
  IndexIterator itr_2 = itr;

  if (isEnd()) {
    return !itr_2.isEnd();
  }

  if (itr_2.isEnd()) {
    return true;
  }

  MappingType m_1 = *itr_1;
  MappingType m_2 = *itr_2;
  KeyType key_1 = m_1.first;
  KeyType key_2 = m_2.first;

  return comparator_(key_1, key_2) != 0;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

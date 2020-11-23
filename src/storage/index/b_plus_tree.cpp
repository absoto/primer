//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  Page *page = FindLeafPage(key, false);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  ValueType value;
  bool key_exists = leaf_page->Lookup(key, &value, comparator_);
  result->push_back(value);
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);

  return key_exists;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }

  return InsertIntoLeaf(key, value);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t page_id;
  Page *page_ = buffer_pool_manager_->NewPage(&page_id);

  if (page_ == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Could not create new page. Out of Memory Exception.");
  }

  root_page_id_ = page_id;
  UpdateRootPageId(1);

  LeafPage *new_leaf_page = reinterpret_cast<LeafPage *>(page_->GetData());
  new_leaf_page->Init(page_id, INVALID_PAGE_ID, leaf_max_size_);
  new_leaf_page->Insert(key, value, comparator_);

  buffer_pool_manager_->UnpinPage(page_id, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  Page *page = FindLeafPage(key, false);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  ValueType val;

  if (leaf_page->Lookup(key, &val, comparator_)) {
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return false;
  }

  int new_size = leaf_page->Insert(key, value, comparator_);

  if (new_size == leaf_page->BPlusTreePage::GetMaxSize()) {
    LeafPage *new_leaf_page = Split(leaf_page, 0);
    InsertIntoParent(leaf_page, new_leaf_page->KeyAt(0), new_leaf_page);
    buffer_pool_manager_->UnpinPage(new_leaf_page->BPlusTreePage::GetPageId(), true);
  }

  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);

  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node, int index) {
  page_id_t page_id;
  Page *new_page_ = buffer_pool_manager_->NewPage(&page_id);

  if (new_page_ == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Could not create new page. Out of Memory Exception.");
  }

  if (node->BPlusTreePage::IsLeafPage()) {
    LeafPage *new_node = reinterpret_cast<LeafPage *>(new_page_->GetData());
    new_node->Init(page_id, node->BPlusTreePage::GetParentPageId(), leaf_max_size_);
    LeafPage *typed_node = reinterpret_cast<LeafPage *>(node);
    typed_node->MoveHalfTo(new_node, buffer_pool_manager_);

    return reinterpret_cast<N *>(new_node);
  }

  InternalPage *new_node = reinterpret_cast<InternalPage *>(new_page_->GetData());
  new_node->Init(page_id, node->BPlusTreePage::GetParentPageId(), internal_max_size_);
  InternalPage *typed_node = reinterpret_cast<InternalPage *>(node);
  typed_node->MoveHalfTo(new_node, index, buffer_pool_manager_);

  return reinterpret_cast<N *>(new_node);
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  page_id_t page_id = new_node->GetPageId();
  page_id_t parent_id;
  Page *page;

  if (old_node->IsRootPage()) {
    page = buffer_pool_manager_->NewPage(&parent_id);

    if (page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "Could not create new root. Out of Memory Exception.");
    }

    InternalPage *new_root = reinterpret_cast<InternalPage *>(page->GetData());
    new_root->Init(parent_id, INVALID_PAGE_ID, internal_max_size_);
    new_root->PopulateNewRoot(old_node->GetPageId(), key, page_id, buffer_pool_manager_);
    root_page_id_ = parent_id;
    UpdateRootPageId(0);

    buffer_pool_manager_->UnpinPage(parent_id, true);
    return;
  }

  parent_id = old_node->GetParentPageId();
  page = buffer_pool_manager_->FetchPage(parent_id);
  InternalPage *parent_page = reinterpret_cast<InternalPage *>(page->GetData());

  page_id_t old_value = parent_page->Lookup(key, comparator_, false);
  int index = parent_page->ValueIndex(old_value);
  int size = parent_page->BPlusTreePage::GetSize();

  if (size + 1 > parent_page->BPlusTreePage::GetMaxSize()) {
    InternalPage *new_page = Split(parent_page, index);

    if (index < parent_page->GetMinSize()) {
      parent_page->InsertNodeAfter(old_value, key, page_id, buffer_pool_manager_);
    } else {
      new_page->InsertNodeAfter(new_page->Lookup(key, comparator_, true), key, page_id, buffer_pool_manager_);
    }

    KeyType new_key = new_page->KeyAt(0);
    InsertIntoParent(parent_page, new_key, new_page);
    buffer_pool_manager_->UnpinPage(parent_id, true);
    buffer_pool_manager_->UnpinPage(new_page->BPlusTreePage::GetPageId(), true);
  } else {
    parent_page->InsertNodeAfter(old_value, key, page_id, buffer_pool_manager_);
    buffer_pool_manager_->UnpinPage(parent_id, true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }

  Page *page = FindLeafPage(key, false);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  int size = leaf_page->RemoveAndDeleteRecord(key, comparator_);

  if (size < leaf_page->BPlusTreePage::GetMinSize()) {
    CoalesceOrRedistribute(leaf_page);
  } else {
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if (node->IsRootPage()) {
    AdjustRoot(node);
    return;
  }

  page_id_t parent_id = node->GetParentPageId();
  Page *page = buffer_pool_manager_->FetchPage(parent_id);
  InternalPage *parent = reinterpret_cast<InternalPage *>(page->GetData());

  int index = parent->ValueIndex(node->GetPageId());

  page_id_t neighbor_id;

  if (index == 0) {
    neighbor_id = parent->ValueAt(index + 1);
  } else {
    neighbor_id = parent->ValueAt(index - 1);
  }

  page = buffer_pool_manager_->FetchPage(neighbor_id);
  N *neighbor_node = reinterpret_cast<N *>(page->GetData());
  bool notComplete = false;
  int isLeaf = 0;

  if (node->IsLeafPage()) {
    isLeaf = 1;
  }

  if (node->GetSize() + neighbor_node->GetSize() + isLeaf > node->GetMaxSize()) {
    Redistribute(neighbor_node, node, parent, index);
  } else {
    notComplete = Coalesce(neighbor_node, node, parent, index);
  }

  if (notComplete) {
    CoalesceOrRedistribute(parent);
  } else {
    buffer_pool_manager_->UnpinPage(parent_id, true);
  }
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be coalesced/redistributed, false means operation is complete
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N *neighbor_node, N *node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent, int index,
                              Transaction *transaction) {
  if (node->BPlusTreePage::IsLeafPage()) {
    LeafPage *neighbor_page = reinterpret_cast<LeafPage *>(neighbor_node);
    LeafPage *page = reinterpret_cast<LeafPage *>(node);

    if (index == 0) {
      neighbor_page->MoveAllTo(page, buffer_pool_manager_);
    } else {
      page->MoveAllTo(neighbor_page, buffer_pool_manager_);
    }
  } else {
    InternalPage *neighbor_page = reinterpret_cast<InternalPage *>(neighbor_node);
    InternalPage *page = reinterpret_cast<InternalPage *>(node);

    if (index == 0) {
      neighbor_page->MoveAllTo(page, parent->KeyAt(1), buffer_pool_manager_);
    } else {
      page->MoveAllTo(neighbor_page, parent->KeyAt(index), buffer_pool_manager_);
    }
  }

  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);

  if (index == 0) {
    buffer_pool_manager_->DeletePage(neighbor_node->GetPageId());
    parent->Remove(1);
  } else {
    buffer_pool_manager_->DeletePage(node->GetPageId());
    parent->Remove(index);
  }

  return parent->BPlusTreePage::GetSize() < parent->BPlusTreePage::GetMinSize();
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node,
                                  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent, int index) {
  if (node->BPlusTreePage::IsLeafPage()) {
    LeafPage *neighbor_page = reinterpret_cast<LeafPage *>(neighbor_node);
    LeafPage *page = reinterpret_cast<LeafPage *>(node);

    if (index == 0) {
      neighbor_page->MoveFirstToEndOf(page);
    } else {
      neighbor_page->MoveLastToFrontOf(page);
    }
  } else {
    InternalPage *neighbor_page = reinterpret_cast<InternalPage *>(neighbor_node);
    InternalPage *page = reinterpret_cast<InternalPage *>(node);

    if (index == 0) {
      neighbor_page->MoveFirstToEndOf(page, parent->KeyAt(1), buffer_pool_manager_);
    } else {
      neighbor_page->MoveLastToFrontOf(page, parent->KeyAt(index), buffer_pool_manager_);
    }
  }

  if (index == 0) {
    parent->SetKeyAt(1, neighbor_node->KeyAt(0));
  } else {
    parent->SetKeyAt(index, node->KeyAt(0));
  }

  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  int size = old_root_node->GetSize();
  bool delete_root = false;

  if (size == 1 && !old_root_node->IsLeafPage()) {
    delete_root = true;
    InternalPage *old_root = reinterpret_cast<InternalPage *>(old_root_node);
    page_id_t new_root_id = old_root->RemoveAndReturnOnlyChild();
    root_page_id_ = new_root_id;

    Page *page = buffer_pool_manager_->FetchPage(new_root_id);
    BPlusTreePage *new_root = reinterpret_cast<BPlusTreePage *>(page->GetData());
    new_root->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(new_root_id, true);
  }

  if (size == 0) {
    delete_root = true;
    root_page_id_ = INVALID_PAGE_ID;
  }

  if (delete_root) {
    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), true);
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
  }

  return delete_root;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  if (IsEmpty()) {
    return end();
  }

  KeyType key;
  Page *page = FindLeafPage(key, true);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());

  return INDEXITERATOR_TYPE(leaf_page, 0, comparator_, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  Page *page = FindLeafPage(key, false);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());

  int index = leaf_page->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(leaf_page, index, comparator_, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { return INDEXITERATOR_TYPE(nullptr, 0, comparator_, buffer_pool_manager_); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage *tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());

  while (!tree_page->BPlusTreePage::IsLeafPage()) {
    InternalPage *internal_page = reinterpret_cast<InternalPage *>(page->GetData());
    page_id_t page_id;

    if (leftMost) {
      page_id = internal_page->ValueAt(0);
    } else {
      page_id = internal_page->Lookup(key, comparator_, false);
    }

    buffer_pool_manager_->UnpinPage(tree_page->GetPageId(), false);
    page = buffer_pool_manager_->FetchPage(page_id);
    tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }

  return page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

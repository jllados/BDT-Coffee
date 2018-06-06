#include <utility>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/identity.hpp>
#include <boost/multi_index/sequenced_index.hpp>

using namespace boost::multi_index;

template <typename Item>

  class mru_list
  {
      typedef multi_index_container<Item,indexed_by<sequenced<>, hashed_unique<identity<Item> >>> item_list;

      private:
         item_list   il;
         std::size_t max_num_items;

      public:
         typedef  Item item_type ;
         typedef typename item_list::iterator iterator;


  mru_list(){}

      int insert(Item item)
      {
		 //if(il.size()>100000) {return 0;}
         std::pair<iterator,bool> p=il.push_front(item);

         if(!p.second){                     /* duplicate item */
             il.relocate(il.begin(),p.first); /* put in front */
          }
       }

      Item  remove()
      { 
         std::pair<int,int> s = il.back();
         il.pop_back();
         return s;
       }

      int size()
	  {
         return il.size();
       }

};
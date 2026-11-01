#include <functional>
#include <queue>
#include <thread>
#include <vector>
#include <condition_variable>
#include <utility>
#include <mutex>
#include <iostream>
#include <atomic>
#include <future>

struct th_pool_t
{
public:
  using f_t = std::function< void() >;
  explicit th_pool_t(unsigned int k);
  ~th_pool_t();
  void submit(f_t f);

  template< class F, class... Args >
  auto submit(F&& f, Args&&... args) -> std::future< decltype(f(args...)) >;

  void wait();
private:
  std::queue< f_t > tasks_;
  std::vector< std::thread > ths_;
  bool stop_{false};
  void execute();
  std::mutex mx_;
  std::condition_variable cd_;
  std::atomic< int > active_tasks_{0};
  std::condition_variable wait_cd_;
};

th_pool_t::th_pool_t(unsigned int k)
{
  auto thf = [this]()
  {
    execute();
  };

  for (unsigned int i = 0; i < k; ++i)
  {
    ths_.emplace_back(thf);
  }
}

th_pool_t::~th_pool_t()
{
  {
    std::lock_guard< std::mutex > lock(mx_);
    stop_ = true; 
  }

  cd_.notify_all();

  for (size_t i = 0; i < ths_.size(); ++i)
  {
    ths_[i].join();
  }
}

void th_pool_t::submit(f_t f)
{
  {
    std::lock_guard< std::mutex > lock(mx_);
    tasks_.push(std::move(f));
    ++active_tasks_;
  }

  cd_.notify_one();
}

template< class F, class... Args >
auto th_pool_t::submit(F&& f, Args&&... args) -> std::future< decltype(f(args...)) >
{
  using return_type = decltype(f(args...));

  auto task_ptr = std::make_shared< std::packaged_task< return_type() > >(
    std::bind(std::forward< F >(f), std::forward< Args >(args)...));

  std::future< return_type > res = task_ptr->get_future();

  {
    std::lock_guard< std::mutex > lock(mx_);
    tasks_.push([task_ptr]()
    {
      (*task_ptr)();
    });
    ++active_tasks_;
  }

  cd_.notify_one();

  return res;
}

void th_pool_t::wait()
{
  std::unique_lock< std::mutex > lock(mx_);
  wait_cd_.wait(lock, [this]()
  {
    return active_tasks_ == 0;
  });
}

void th_pool_t::execute()
{
  std::unique_lock< std::mutex > lck(mx_);
  do
  {
    auto proceed = [&]()
    {
      return !tasks_.empty() || stop_;
    };

    cd_.wait(lck, proceed);

    if (tasks_.empty())
    {
      continue;
    }

    auto task = std::move(tasks_.front());
    tasks_.pop();

    lck.unlock();

    try
    {
      task();
    }
    catch (...)
    {}
    
    lck.lock();

    if (--active_tasks_ == 0)
    {
      wait_cd_.notify_all();
    }
  }
  while (!stop_ || !tasks_.empty());
}


int main()
{
  th_pool_t pool(5);

  auto fut1 = pool.submit([]()
  {
    std::cout << "Hello from task!\n";
  });

  auto fut2 = pool.submit([]()
  {
    return 42;
  });

  auto fut3 = pool.submit([](int a, int b)
  {
    return a + b;
  }, 10, 20);

  std::cout << "fut2 = " << fut2.get() << '\n';
  std::cout << "fut3 = " << fut3.get() << '\n';

  fut1.get();

  pool.wait();
  std::cout << "Done!\n";
}

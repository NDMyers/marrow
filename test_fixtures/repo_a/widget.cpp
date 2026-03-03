#include <string>

class Widget {
public:
    Widget(const std::string& name) : name_(name) {}
    std::string getName() const { return name_; }
private:
    std::string name_;
};

void processWidget(const Widget& w) {
    auto name = w.getName();
}

All the libraries in examples-libs are outdated, as is the dependencies.txt listing and
instructions about ZooInspector.

I don't have time to update all this right now, plus I would not want to keep storing JARs in here
and would use a better way, e.g. use Kotlin scripting with the ability to have the script pull
its required dependencies.

For now, I am leaving all this as-is, but I am about 99.999% sure the examples won't work without
changes, since I just updated all the dependencies in the POM, but the example scripts are still
using the old JARs.
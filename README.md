**This is now archived, since the presentation was back in 2012 and the code hasn't really changed, and doesn't need to.**

This is code that accompanies a talk I gave on Apache ZooKeeper.

The slides can be found at http://www.slideshare.net/scottleber/apache-zookeeper

May 2017 Update:
* Updated ZooKeeper version to 3.4.10
* Updated Curator version to 2.12.0
* Changed all System.out to SLFJ Loggers
* A bunch of misc. code cleanup, e.g. converting anonymous inner classes to lambdas, using diamond operator, etc.

October 2023 update:
* Update Zookeeper version to 3.9.1
* Update Curator version to 5.5.0
* Update SLF4J version to 2.0.9 and remove SLF4J to Log4J bridge
* Update to use JDK 17
* Update plugins and several other dependencies, clean up the POM, etc.
* Switch to use JUnit 5 (Jupiter) and AssertJ assertions
* Remove test support classes EmbeddedZooKeeperServer and EmbeddedZooKeeperServerRule; they cause the tests to hang, and it's better to use Curator's testing server anyway

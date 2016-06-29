#mvn clean install deploy -s $HOME/.m2/settings-search-release.xml -DskipTests=true -Dcheckstyle.skip=true -Dmaven.compiler.useIncrementalCompilation=false -DmongodbProvider=external
mvn clean install -DskipTests -Dcheckstyle.skip=true

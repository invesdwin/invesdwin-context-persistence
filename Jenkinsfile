pipeline {
  agent any
  stages {
    stage('Build and test') {
      steps{
        withMaven {
          sh 'mvn clean install -f invesdwin-context-persistence-parent/pom.xml -T4'
        }  
      }
    }
  }
}
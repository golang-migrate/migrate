
// This library defines the isPrBuild, prepareBuild and finalizeBuild methods
@Library('jenkins.shared.library') _

pipeline {
  agent {
    label 'ubuntu_docker_label'
  }
  tools {
    go "Go 1.24.2"
  }
  options {
    checkoutToSubdirectory('src/github.com/infobloxopen/migrate')
  }
  environment {
    GOPATH = "$WORKSPACE"
    DIRECTORY = "src/github.com/infobloxopen/migrate"
  }

  stages {
    stage("Setup") {
      steps {
        // prepareBuild is one of the Secure CICD helper methods
        prepareBuild()
      }
    }
    stage("Unit Tests") {
      steps {
        dir("$DIRECTORY") {
          // sh "make test"
        }
      }
    }
    stage("Build Image") {
      // only build images on trunk builds. An alternate approach
      // when { branch 'main' } or when { anyOf { branch "main", branch "develop", "ib" } }
      when {
        expression { ! isPrBuild() }
      }
      steps {
        withDockerRegistry([credentialsId: "${env.JENKINS_DOCKER_CRED_ID}", url: ""]) {
          dir("$DIRECTORY") {
            sh "make build"
          }
        }
      }
    }
  }
  post {
    success {
      // finalizeBuild is one of the Secure CICD helper methods
      dir("$DIRECTORY") {
        finalizeBuild(
          sh(
            script: 'make list-of-images',
            returnStdout: true
          )
        )
      }
    }
    cleanup {
      dir("$DIRECTORY") {
        sh "make clean || true"
      }
      cleanWs()
    }
  }
}

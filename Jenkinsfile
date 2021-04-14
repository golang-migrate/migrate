
// This library defines the isPrBuild, prepareBuild and finalizeBuild methods
@Library('jenkins.shared.library') _

pipeline {
  agent {
    label 'ubuntu_docker_label'
  }
  tools {
    go "Go 1.14.4"
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
          sh "make test"
        }
      }
    }
    stage("Build Image") {
      steps {
        dir("$DIRECTORY") {
          sh "make build"
        }
      }
    }
    stage("Push Image") {
      // only push images on trunk builds. An alternate approach
      // when { branch 'main' } or when { anyOf { branch "main", branch "develop" } }
      when {
        expression { ! isPrBuild() }
      }
      steps {
        // reference Jenkins credential ids via an environment variable
        withDockerRegistry([credentialsId: "${env.JENKINS_DOCKER_CRED_ID}", url: ""]) {
          dir("$DIRECTORY") {
            sh "make docker-push"
          }
        }
      }
    }
  }
  post {
    success {
      // finalizeBuild is one of the Secure CICD helper methods
      dir("$DIRECTORY") {
          finalizeBuild()
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

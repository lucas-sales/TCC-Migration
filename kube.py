import logging
import time

from kubernetes import client, config
from kubernetes.client import ApiException

from src.exceptions.kube import KubernetesException, KubernetesDeploymentNotFoundException
from src.config import settings

log = logging.getLogger(__name__)


class KubeOperation:
    KUBE_ERROR_TYPE = 'ReplicaFailure'
    KUBE_AVAILABLE_TYPE = 'Available'
    KUBE_PROGRESSING_TYPE = 'Progressing'

    def __init__(self, deploy_name: str, namespace: str):
        config.load_kube_config(settings.KUBE_CONFIG)

        self.deploy_name = deploy_name
        self.namespace = namespace
        self.client = client
        # Config client
        self.deploy_client = self.client.AppsV1Api()

        # original replicas
        self.resource = self._get_deployment_info()
        self.num_replicas_before = self.resource.spec.replicas
        self.current_version = int(self.resource.metadata.generation)

    def _get_deployment_info(self):
        try:
            deploy_obj = self.deploy_client.read_namespaced_deployment(name=self.deploy_name,
                                                                       namespace=self.namespace)
            return deploy_obj
        except ApiException as e:
            if e.reason == 'Not Found':
                log.info(f'Deployment: "{self.deploy_name}" invalid or not found!')
                raise KubernetesDeploymentNotFoundException
            raise e

    def _get_deployment_status(self):
        self.resource = self._get_deployment_info()
        status = self.resource.status.conditions
        return [str(st.type) for st in status]

    def _set_pod_replica(self, replica_number: int):
        tries = 0
        while True:
            try:
                self.deploy_client.patch_namespaced_deployment(
                    namespace=self.namespace,
                    name=self.deploy_name,
                    body={'spec': {'replicas': replica_number}},
                )
                break
            except Exception as e:
                if tries >= 60:
                    # in case of not recovering from exception, raise
                    raise KubernetesException(e)
                tries += 1
                time.sleep(1)

        patched = self._get_deployment_info()
        self.current_version = int(patched.metadata.generation)
        self.resource = patched

    def _update_pod_affinity(self, values: list):
        terms = self.client.models.V1NodeSelectorTerm(
            match_expressions=[
                {'key': 'kubernetes.io/hostname',
                 'operator': 'In',
                 'values': values}
            ]
        )
        node_affinity = self.client.models.V1NodeAffinity(
            preferred_during_scheduling_ignored_during_execution=[
                self.client.models.V1PreferredSchedulingTerm(preference=terms, weight=1)
            ],
        )

        affinity = self.client.models.V1Affinity(node_affinity=node_affinity)

        tries = 0
        while True:
            # replace affinity in the deployment object
            self.resource.spec.template.spec.affinity = affinity
            try:
                # finally, push the updated deployment configuration to the API-server
                self.deploy_client.replace_namespaced_deployment(name=self.deploy_name,
                                                                 namespace=self.namespace,
                                                                 body=self.resource)
                break
            except Exception as e:
                if tries >= 60:
                    # in case of not recovering from exception, recover pod replicas
                    self._set_pod_replica(replica_number=self.num_replicas_before)
                    raise KubernetesException(e)
                tries += 1
                time.sleep(1)
                updated = self._get_deployment_info()
                self.current_version = int(updated.metadata.generation)
                self.resource = updated

        self.resource = self._get_deployment_info()

    def reallocate_pod(self, values: list):
        try:
            log.info(f'Trying to update {self.deploy_name} with soft mode...')
            self._update_pod_affinity(values=values)
            time.sleep(3)
            status_list = self._get_deployment_status()
            if self.KUBE_ERROR_TYPE in status_list:
                log.info(f"Soft mode in {self.deploy_name} didn't work!")
                log.info(f'Trying to update {self.deploy_name} with hard mode...')
                log.info(f'Setting replicas of {self.deploy_name} to zero...')
                self._set_pod_replica(replica_number=0)
                time.sleep(1)
                log.info(f'Setting replicas of {self.deploy_name} to original value...')
                self._set_pod_replica(replica_number=self.num_replicas_before)

            log.info(f'Awaiting deployment {self.deploy_name} recover...')
            status_list = self._get_deployment_status()
            tries = 0
            while self.KUBE_ERROR_TYPE in status_list:
                if tries >= 60:
                    logging.error(f"Deployment {self.deploy_name} isn't recovering from {self.KUBE_ERROR_TYPE}")
                    raise KubernetesException()
                log.info(f'Awaiting...')
                tries += 1
                time.sleep(1)
                status_list = self._get_deployment_status()
            log.info(f'Operation for deployment {self.deploy_name} complete!')
            return {'deployment_name': self.deploy_name, 'status': True}
        except KubernetesException:
            return {'deployment_name': self.deploy_name, 'status': False}

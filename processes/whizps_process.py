from py_rpc_producer.geoserver_interface import generate_wms_from_tiffs, \
    get_geoserver_wms_endpoint
from py_rpc_producer.py_rpc_producer import handler_method
from pywps import Process, ComplexOutput, ComplexInput, Format, LiteralInput
from pywps.inout.outputs import WMSOutput


# Message queueing (RabbitMQ) settings
MQ_HOST = "localhost"
MQ_PORT = 5678
MQ_USER = "guest"
MQ_PASSWORD = "guest"
WORKER_QUEUE_NAME = "whizps_task_queue"
MQ_LOGGING_KEYWORD = "STATUS"
MQ_END_OF_PROCESS_KEYWORD = "process finished"
MAX_JOB_ACKNOWLEDGE_WAITING_TIME = 60

# Geoserver settings
GS_HOST = "localhost"
GS_PORT = 8080
GS_USER = "admin"
GS_PASSWORD = "geoserver"
GS_WORKSPACE = "WHIZPS"
GS_STORE_NAME_PREFIX = "whizps_"


class WhizPSprocess(Process):
    def __init__(self):
        inputs = [
            ComplexInput('xml', 'xml input',
                         supported_formats=[Format('application/xml+gml')]),
            LiteralInput('test', 'test literal input', data_type='string')
        ]

        outputs = [
            WMSOutput('wms', 'Reference to wms service',
                      supported_formats=[Format('application/x-ogc-wms')],
                      as_reference=True),
            ComplexOutput('result_pdf', 'Reference to ouput pdf',
                         supported_formats=[Format('application/pdf')],
                         as_reference=True)
        ]

        super(WhizPSprocess, self).__init__(
            self._handler,
            identifier='whizps',
            title='WhizPS demo process',
            abstract='Demo process that utilizes WhizPS architecture',
            version='0.1',
            inputs=inputs,
            outputs=outputs,
            store_supported=True,
            status_supported=True
        )

    def _handler(self, request, response):
        """
        called via the process. Uses the wps rpc client to forward request
        to simulation client.
        :param request: pywps request
        :param response: pswps response object (used for status update etc)
        :return: the response object
        """

        rpc_client, output_data_dict = handler_method(
            MQ_HOST, MQ_PORT, MQ_USER, MQ_PASSWORD,
            response, request, self.uuid,
            logging_keyword="STATUS",
            end_of_process_keyword="process finished",
            worker_queue_name=WORKER_QUEUE_NAME,
            max_acknowledge_waiting_time=60)

        gs_wms_addr = get_geoserver_wms_endpoint(GS_HOST, GS_PORT, GS_WORKSPACE)

        wms_store_name = generate_wms_from_tiffs(output_data_dict, self.workdir,
                                                 self.uuid, GS_HOST, GS_PORT,
                                                 GS_WORKSPACE, GS_USER,
                                                 GS_PASSWORD, GS_STORE_NAME_PREFIX)

        # add wms to response object
        response.outputs['wms'].data = ""
        response.outputs['wms'].layer = GS_WORKSPACE + ":" + wms_store_name
        response.outputs['wms'].wms_addr = gs_wms_addr

        # add result pdf to output
        for key, value in output_data_dict.items():
            if str(key).endswith(".pdf"):
                response.outputs['result_pdf'].data = value

        return response

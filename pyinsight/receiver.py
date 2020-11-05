import os
import json
import logging
import threading
import pyinsight
from pyinsight.action import backlog
from pyinsight.utils.exceptions import InsightDataSpecError
from pyinsight.transfer import Transfer
from pyinsight.utils.validation import x_i_proto_check
from pyinsight.utils.core import get_merge_level, get_data_chunk, encoder


__all__ = ['Receiver']

"""
Receive and Dispatch Message, Save the message to Dispositor and Archiver
"""
class Receiver(Transfer):
    @backlog
    def receive_data(self, header, data):
        # 1. Receive and check
        # 1.1 Checks
        x_i_proto_check(header, data)
        # 1.2 Context Setting
        self.depositor.set_current_topic_table(header['topic_id'], header['table_id'])
        self.archiver.set_current_topic_table(header['topic_id'], header['table_id'])
        client_set = self.subscription_dict.get((header['topic_id'], header['table_id']), [])
        dispatch_body_data = list()
        self.log_context['context'] = '-'.join([self.depositor.topic_id, self.depositor.table_id])
        # 1.3 Set Some useful flags for no-header items
        if int(header.get('age', 0)) != 1:
            merge_key = str(int(header['start_seq']) + int(header.get('age',0)))
            header.update({'merge_status':'initial', 'merge_key': merge_key})
            header['merge_level'] = get_merge_level(merge_key)
        # 1.4 Header data related actions
        else:
            self.messager.trigger_clean(header['topic_id'], header['table_id'], header['start_seq'])
            header['merge_status'] = 'header'
            header['merge_level'] = 9
        # 2. Encoder
        # 2.1 Depositor Scope
        if header['data_store'] == 'body':
            prepared_data = encoder(data, header['data_encode'], self.depositor.data_encode)
            header['data_encode'] = self.depositor.data_encode
            if client_set:
                dispatch_body_data = json.loads(encoder(prepared_data, header['data_encode'], 'flat'))
        # 2.2 Archiver Scope
        elif header['data_store'] == 'file':
            self.archiver.set_merge_key(header['merge_key'])
            # Step 1: Get the dict data of 'x-i-a' specification
            raw_data = self.archiver.read_data_from_file(header['data_encode'], header['data_format'], data)
            # Step 2: Cut the data and reload the receive data in "Depositor Mode"
            header['data_store'] = 'body'
            header['data_encode'] = 'flat'
            for chunk_header in get_data_chunk(raw_data, header, self.merge_size):
                chunk_data = chunk_header.pop('data')
                self.logger.info("Receiving chunk {}-{}".format(header['start_seq'], header.get('age', 0)),
                                 extra=self.log_context)
                self.receive_data(chunk_header, chunk_data)
            return True
        else:
            raise InsightDataSpecError("INS-000004")
        # 3. Send client messages via multi-threading
        handlers = list()
        for client_id in list(client_set):
            dispatcher = self.client_dict.get(client_id, None)
            if dispatcher and isinstance(dispatcher, pyinsight.dispatcher.Dispatcher):
                dispatcher.set_merge_size(self.merge_size)
                cur_handler = threading.Thread(target=self.dispatch_data,
                                               args=(dispatcher, header, dispatch_body_data))
                self.logger.info("Dispatching to client {}".format(client_id), extra=self.log_context)
                cur_handler.start()
                handlers.append(cur_handler)
            else:
                self.logger.warning("No dispatcher for client {}".format(client_id), extra=self.log_context)
        # 4. Document to be saved by Depositor - Synchronized Update
        self.logger.info("Deposit document", extra=self.log_context)
        self.depositor.add_document(header, prepared_data)
        self.archiver.remove_data()
        # 5. Check if the first level merge process should be triggered
        if header.get('merge_level', 0) > 0 and header.get('merge_status', '') != 'header':
            self.logger.info("Trigger Merging", extra=self.log_context)
            self.messager.trigger_merge(header['topic_id'], header['table_id'],
                                        header['merge_key'], 1)
        # 6. Wait until all the dispatch thread are finished
        for handler in handlers:
            handler.join()
        return True
import unittest
from unittest.mock import Mock, MagicMock, patch
from uuid import uuid4
import io
import struct
from ..standalone_file_storage import StandaloneFileStorage, WALState, AtomPointer, ProtoValidationException, \
    ProtoUnexpectedException


class TestStandaloneFileStorage(unittest.TestCase):

    def setUp(self):
        """
        Configuración inicial antes de cada test.
        """
        # Crear un mock para el `BlockProvider`
        self.mock_block_provider = Mock()
        self.mock_block_provider.get_new_wal = MagicMock(return_value=(uuid4(), 0))
        self.mock_block_provider.write_streamer = MagicMock()
        self.mock_block_provider.get_reader = MagicMock()
        self.mock_block_provider.close_wal = MagicMock()

        # Crear una instancia de StandaloneFileStorage con el mock
        self.storage = StandaloneFileStorage(
            block_provider=self.mock_block_provider,
            buffer_size=1024,  # Usar un buffer pequeño para pruebas.
            blob_max_size=1024 * 10  # Máximo de 10KB para prueba.
        )

    def test_init_storage(self):
        """
        Test de inicialización correcta de la clase.
        """
        self.assertEqual(self.storage.state, 'Running')
        self.assertIsNotNone(self.storage.executor_pool)
        self.assertEqual(self.storage.current_wal_id, self.mock_block_provider.get_new_wal.return_value[0])

    def test_push_bytes_within_limits(self):
        """
        Test para `push_bytes` cuando los datos caben en el buffer actual.
        """
        data = bytes(b"A" * 512)  # 512 bytes de datos.
        transaction_id, offset = self.storage.push_bytes(data).result()

        # Verificar que se asignaron correctamente los IDs y offsets.
        self.assertEqual(transaction_id, self.storage.current_wal_id)
        self.assertEqual(offset, self.storage.current_wal_base)

    def test_push_bytes_exceeding_buffer(self):
        """
        Test para `push_bytes` cuando los datos exceden el tamaño del buffer.
        """
        data = bytes(b"A" * (2048 - 8))  # 2048 bytes (excede buffer de 1024).
        transaction_id, offset = self.storage.push_bytes(data).result()

        # Verificar que el método fragmentó los datos adecuadamente.
        self.assertEqual(self.storage.current_wal_id, transaction_id)
        self.assertEqual(offset, 0)

        data = bytes(b"Prueba")  # 2048 bytes (excede buffer de 1024).
        transaction_id, offset = self.storage.push_bytes(data).result()

        self.assertEqual(offset, 2048)

        data = bytes(b"Prueba")  # 2048 bytes (excede buffer de 1024).
        transaction_id, offset = self.storage.push_bytes(data).result()

        self.assertEqual(offset, 2048 + len("Prueba") + 8)

    def test_push_bytes_exceeding_blob_max_size(self):
        """
        Test para `push_bytes` cuando los datos exceden el tamaño máximo permitido.
        """
        data = bytes(b"A" * (self.storage.blob_max_size + 1))  # 1 byte más que el tamaño máximo permitido.
        with self.assertRaises(ProtoValidationException):
            self.storage.push_bytes(data)

    def test_flush_wal(self):
        """
        Test para `flush_wal`, verificando que los datos se pasan al `block_provider`.
        """
        data = bytes(b"Test Data")
        self.storage.push_bytes(data)
        self.storage.flush_wal()

        # Verificar que el flush escribe los datos pendientes.
        self.assertTrue(self.mock_block_provider.write_streamer.called)

    def test_push_atom(self):
        """
        Test para `push_atom`. Verifica que se serializan los objetos `Atom` y se almacenan correctamente.
        """
        # Mock del Atom
        fake_atom = {"attr1": "value1", "attr2": 123}

        future = self.storage.push_atom(fake_atom)
        atom_pointer = future.result()

        # Verificar que el resultado es un AtomPointer.
        self.assertIsInstance(atom_pointer, AtomPointer)
        self.assertEqual(atom_pointer.transaction_id, self.storage.current_wal_id)

    def test_get_atom(self):
        """
        Test para `get_atom`. Recupera un Atom desde el almacenamiento.
        """
        # Configurar el mock para simular la recuperación de un Atom.
        fake_data = '{"className": "FakeAtom", "attr1": "value1", "attr2": 123}'
        fake_data_len: int = len(fake_data)
        len_data = struct.pack('Q', fake_data_len)

        self.mock_block_provider.get_reader.return_value = io.BytesIO(len_data + fake_data.encode("utf-8"))

        # Crear un AtomPointer simulado
        fake_pointer = AtomPointer(transaction_id=uuid4(), offset=0)

        future = self.storage.get_atom(fake_pointer)
        atom = future.result()

        # Verificar que el objeto recuperado es un Atom correctamente construido.
        self.assertEqual(atom['attr1'], "value1")
        self.assertEqual(atom['attr2'], 123)

    def test_restore_state(self):
        """
        Test para `_restore_state`.
        """
        state = WALState(
            wal_buffer=[bytearray(b"Test Data")],
            wal_offset=5,
            wal_base=10
        )
        self.storage._restore_state(state)

        # Verificar que el estado del WAL fue restaurado correctamente.
        self.assertEqual(self.storage.current_wal_buffer, state.wal_buffer)
        self.assertEqual(self.storage.current_wal_offset, state.wal_offset)
        self.assertEqual(self.storage.current_wal_base, state.wal_base)

    def test_flush_wal_with_pending_writes(self):
        """
        Test para `flush_wal`, verificando que las operaciones pendientes se procesen correctamente.
        """
        data = bytes(b"{'className': 'prueba'")
        self.storage.push_bytes(data)

        # Simular múltiples escrituras pendientes en el WAL.
        self.storage.flush_wal()
        self.assertEqual(len(self.storage.pending_writes), 0)
        self.mock_block_provider.write_streamer.assert_called_once()

    def test_close_storage(self):
        """
        Test para `close`. Verifica el correcto cierre del almacenamiento.
        """
        with patch.object(self.storage.executor_pool, 'shutdown') as mock_shutdown:
            self.storage.close()

            # Verificar que el estado cambia a "Closed".
            self.assertEqual(self.storage.state, 'Closed')
            self.assertTrue(mock_shutdown.called)
            self.mock_block_provider.close.assert_called_once()

    def test_push_bytes_when_storage_not_running(self):
        """
        Test para verificar que `push_bytes` lanza una excepción cuando el almacenamiento no está activo.
        """
        self.storage.state = 'Closed'
        data = bytearray(b"Test Data")
        with self.assertRaises(ProtoValidationException):
            self.storage.push_bytes(data)


if __name__ == '__main__':
    unittest.main()

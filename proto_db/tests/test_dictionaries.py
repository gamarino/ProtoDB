import unittest
import uuid
from ..dictionaries import Dictionary, DictionaryItem, Atom  # Sustituye 'your_module' por el nombre del módulo correcto


class TestDictionary(unittest.TestCase):

    def setUp(self):
        """
        Configuración inicial para las pruebas.
        """
        self.empty_dictionary = Dictionary()  # Diccionario vacío
        self.atom_a = Atom()  # Simula un Atom como valor de prueba
        self.atom_b = Atom()
        self.atom_c = Atom()

    # --- Test set_at ---
    def test_set_at(self):
        # Insertar en un diccionario vacío
        updated_dict = self.empty_dictionary.set_at("10", self.atom_a)
        self.assertEqual(updated_dict.get_at("10"), self.atom_a, "Should insert value at key 10.")

        # Insertar un valor que requiere rebalanceo
        updated_dict = updated_dict.set_at("5", self.atom_b)
        self.assertEqual(updated_dict.get_at("5"), self.atom_b, "Should insert value at key 5.")

        # Insertar un valor que ya existe
        updated_dict = updated_dict.set_at("10", self.atom_c)
        self.assertEqual(updated_dict.get_at("10"), self.atom_c, "Should update the value of an existing key.")

    # --- Test get_at ---
    def test_get_at(self):
        # Diccionario vacío
        self.assertIsNone(self.empty_dictionary.get_at("10"), "Should return None for an empty dictionary.")

        # Insertar nodo y comprobar búsqueda
        updated_dict = self.empty_dictionary.set_at("10", self.atom_a)
        self.assertEqual(updated_dict.get_at("10"), self.atom_a, "Should return the correct value for the key.")

        # Búsqueda de clave inexistente
        self.assertIsNone(updated_dict.get_at("20"), "Should return None for a non-existent key.")

    # --- Test remove_key ---
    def test_remove_at(self):
        content = self.empty_dictionary
        content = content.set_at("10", self.atom_a)
        content = content.set_at("20", self.atom_b)
        content = content.set_at("15", self.atom_c)

        content = content.remove_at("20")
        self.assertEqual(content.count, 2, "Should has a count of 2 at this point")
        self.assertIsNone(content.get_at("20"), "Should remove the key 20 from the content.")

    # --- Test as_iterable ---
    def test_as_iterable(self):
        content = self.empty_dictionary.set_at("10", self.atom_a)
        content = content.set_at("20", self.atom_b)
        content = content.set_at("15", self.atom_c)
        content = content.set_at("30", None)

        result = [(r[0], r[1]) for r in content.as_iterable()]
        self.assertEqual(len(result), 4, "Should include all inserted keys in the iterable.")
        self.assertIn(("10", self.atom_a), result, "Should include the correct key-value pair.")

        self.assertEqual(result[0], ("10", self.atom_a), "Should include the correct key-value pair.")
        self.assertEqual(result[1], ("15", self.atom_c), "Should include the correct key-value pair.")
        self.assertEqual(result[2], ("20", self.atom_b), "Should include the correct key-value pair.")
        self.assertEqual(result[3], ("30", None), "Should include the correct key-value pair.")

    # --- Test Dictionary methods ---
    def test_dictionary_methods(self):
        # Crear un diccionario vacío
        dictionary = Dictionary()

        # Agregar valores
        dictionary1 = dictionary.set_at("key1", self.atom_a)
        dictionary2 = dictionary1.set_at("key2", self.atom_b)
        self.assertIs(
            dictionary2.get_at("key1"),
            self.atom_a,
            "Should return the correct value for key 'key1'."
        )
        self.assertIs(
            dictionary2.get_at("key2"),
            self.atom_b,
            "Should return the correct value for key 'key2'."
        )

        # Comprobar si tiene claves
        self.assertTrue(dictionary2.has("key1"), "Should confirm the presence of 'key1'.")
        self.assertFalse(dictionary2.has("keyX"), "Should confirm 'keyX' is not present.")

        # Eliminar una clave
        dictionary3 = dictionary2.remove_at("key1")
        self.assertFalse(dictionary3.has("key1"), "Should confirm 'key1' is removed.")

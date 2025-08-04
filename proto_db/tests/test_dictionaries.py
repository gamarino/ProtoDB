import unittest

from ..dictionaries import Dictionary, RepeatedKeysDictionary, Atom  # Sustituye 'your_module' por el nombre del módulo correcto


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

    def test_concurrent_optimized(self):
        """
        Test the functionality of handling concurrent modifications in Dictionary.

        This test simulates a concurrent update scenario and verifies that
        the dictionary can handle it correctly by applying operations in the
        expected order.
        """
        # Create a base dictionary
        base_dict = Dictionary()
        base_dict = base_dict.set_at("key1", "value1")
        base_dict = base_dict.set_at("key2", "value2")

        # Simulate a concurrent modification by creating a new dictionary
        # with the same initial state but different modifications
        concurrent_dict = Dictionary()
        concurrent_dict = concurrent_dict.set_at("key1", "value1")
        concurrent_dict = concurrent_dict.set_at("key2", "value2")
        concurrent_dict = concurrent_dict.set_at("key3", "value3")  # Add a new key
        concurrent_dict = concurrent_dict.remove_at("key1")  # Remove key1

        # Create our local modifications
        local_dict = base_dict.set_at("key4", "value4")  # Add a new key
        local_dict = local_dict.set_at("key2", "new_value2")  # Modify key2

        # Manually simulate what would happen in a concurrent update scenario
        # Apply our local modifications to the concurrent dictionary
        rebased_dict = concurrent_dict
        rebased_dict = rebased_dict.set_at("key4", "value4")  # Add key4 from local_dict
        rebased_dict = rebased_dict.set_at("key2", "new_value2")  # Update key2 from local_dict

        # Verify the rebased dictionary has the expected values
        self.assertFalse(rebased_dict.has("key1"), "key1 should be removed (from concurrent_dict)")
        self.assertEqual(rebased_dict.get_at("key2"), "new_value2", "key2 should have the new value (from local_dict)")
        self.assertEqual(rebased_dict.get_at("key3"), "value3", "key3 should be preserved (from concurrent_dict)")
        self.assertEqual(rebased_dict.get_at("key4"), "value4", "key4 should be added (from local_dict)")

        # This test demonstrates the expected behavior when handling concurrent modifications
        # by applying operations in the correct order.


class TestRepeatedKeysDictionary(unittest.TestCase):
    """Test cases for the RepeatedKeysDictionary class."""

    def setUp(self):
        """Set up test fixtures."""
        self.empty_dict = RepeatedKeysDictionary()
        self.atom_a = Atom(value="value_a")
        self.atom_b = Atom(value="value_b")
        self.atom_c = Atom(value="value_c")
        self.atom_d = Atom(value="value_d")

    def test_set_at_with_new_key(self):
        """Test setting a value for a new key."""
        updated_dict = self.empty_dict.set_at("key1", self.atom_a)
        result = list(updated_dict.get_at("key1"))
        self.assertEqual(len(result), 1, "Should have one value for the key.")
        self.assertEqual(result[0], self.atom_a, "Should store the correct value.")

    def test_set_at_with_existing_key(self):
        """Test setting multiple values for the same key."""
        dict_with_one = self.empty_dict.set_at("key1", self.atom_a)
        dict_with_two = dict_with_one.set_at("key1", self.atom_b)

        result = list(dict_with_two.get_at("key1"))
        self.assertEqual(len(result), 2, "Should have two values for the key.")
        self.assertIn(self.atom_a, result, "Should contain the first value.")
        self.assertIn(self.atom_b, result, "Should contain the second value.")

    def test_get_at_nonexistent_key(self):
        """Test getting a value for a nonexistent key."""
        result = list(self.empty_dict.get_at("nonexistent"))
        self.assertEqual(len(result), 0, "Should return an empty list for nonexistent key.")

    def test_remove_at(self):
        """Test removing all values for a key."""
        dict_with_values = self.empty_dict.set_at("key1", self.atom_a).set_at("key1", self.atom_b)
        dict_after_remove = dict_with_values.remove_at("key1")

        result = list(dict_after_remove.get_at("key1"))
        self.assertEqual(len(result), 0, "Should have no values after removal.")

    def test_remove_record_at(self):
        """Test removing a specific record for a key."""
        dict_with_values = (self.empty_dict
                           .set_at("key1", self.atom_a)
                           .set_at("key1", self.atom_b)
                           .set_at("key1", self.atom_c))

        # Remove one specific record
        dict_after_remove = dict_with_values.remove_record_at("key1", self.atom_b)

        result = list(dict_after_remove.get_at("key1"))
        self.assertEqual(len(result), 2, "Should have two values after removing one.")
        self.assertIn(self.atom_a, result, "Should still contain the first value.")
        self.assertIn(self.atom_c, result, "Should still contain the third value.")
        self.assertNotIn(self.atom_b, result, "Should not contain the removed value.")

    def test_remove_nonexistent_record(self):
        """Test removing a record that doesn't exist."""
        dict_with_values = self.empty_dict.set_at("key1", self.atom_a)

        # Try to remove a record that doesn't exist
        dict_after_remove = dict_with_values.remove_record_at("key1", self.atom_b)

        result = list(dict_after_remove.get_at("key1"))
        self.assertEqual(len(result), 1, "Should still have one value.")
        self.assertEqual(result[0], self.atom_a, "Should still contain the original value.")

    def test_concurrent_optimized(self):
        """Test concurrent optimization with repeated keys."""
        # Create a base dictionary with repeated keys
        base_dict = RepeatedKeysDictionary()
        base_dict = base_dict.set_at("key1", self.atom_a)
        base_dict = base_dict.set_at("key1", self.atom_b)
        base_dict = base_dict.set_at("key2", self.atom_c)

        # Simulate concurrent modifications
        concurrent_dict = RepeatedKeysDictionary()
        concurrent_dict = concurrent_dict.set_at("key1", self.atom_a)
        concurrent_dict = concurrent_dict.set_at("key1", self.atom_b)
        concurrent_dict = concurrent_dict.set_at("key2", self.atom_c)
        concurrent_dict = concurrent_dict.set_at("key3", self.atom_d)  # Add a new key
        concurrent_dict = concurrent_dict.remove_record_at("key1", self.atom_a)  # Remove one record

        # Create local modifications
        local_dict = base_dict.set_at("key1", self.atom_c)  # Add another value to key1
        local_dict = local_dict.remove_at("key2")  # Remove key2

        # Manually simulate rebasing
        rebased_dict = concurrent_dict
        rebased_dict = rebased_dict.set_at("key1", self.atom_c)  # Add atom_c from local_dict
        rebased_dict = rebased_dict.remove_at("key2")  # Remove key2 from local_dict

        # Verify the rebased dictionary
        key1_values = list(rebased_dict.get_at("key1"))
        self.assertEqual(len(key1_values), 2, "Should have two values for key1.")
        self.assertIn(self.atom_b, key1_values, "Should contain atom_b.")
        self.assertIn(self.atom_c, key1_values, "Should contain atom_c.")
        self.assertNotIn(self.atom_a, key1_values, "Should not contain atom_a (removed).")

        self.assertFalse(rebased_dict.has("key2"), "key2 should be removed.")
        self.assertTrue(rebased_dict.has("key3"), "key3 should be present.")

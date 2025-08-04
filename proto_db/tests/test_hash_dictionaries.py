import unittest

from ..dictionaries import Dictionary, Atom
from ..hash_dictionaries import HashDictionary


class TestHashDictionary(unittest.TestCase):

    def setUp(self):
        """
        Configuración inicial para las pruebas.
        """
        self.empty_dictionary = HashDictionary()  # Diccionario vacío
        self.atom_a = Atom()  # Simula un Atom como valor de prueba
        self.atom_b = Atom()
        self.atom_c = Atom()

    # --- Test set_at ---
    def test_set_at(self):
        # Insertar en un diccionario vacío
        updated_dict = self.empty_dictionary.set_at(10, self.atom_a)
        self.assertEqual(updated_dict.get_at(10), self.atom_a, "Should insert value at key 10.")

        # Insertar un valor que requiere rebalanceo
        updated_dict = updated_dict.set_at(5, self.atom_b)
        self.assertEqual(updated_dict.get_at(5), self.atom_b, "Should insert value at key 5.")

        # Insertar un valor que ya existe
        updated_dict = updated_dict.set_at(10, self.atom_c)
        self.assertEqual(updated_dict.get_at(10), self.atom_c, "Should update the value of an existing key.")

    # --- Test get_at ---
    def test_get_at(self):
        # Diccionario vacío
        self.assertIsNone(self.empty_dictionary.get_at(10), "Should return None for an empty dictionary.")

        # Insertar nodo y comprobar búsqueda
        updated_dict = self.empty_dictionary.set_at(10, self.atom_a)
        self.assertEqual(updated_dict.get_at(10), self.atom_a, "Should return the correct value for the key.")

        # Búsqueda de clave inexistente
        self.assertIsNone(updated_dict.get_at(20), "Should return None for a non-existent key.")

    # --- Test _rebalance ---
    def test_rebalance(self):
        # Nodo balanceado
        root = HashDictionary(key=10, value=self.atom_a)
        left = HashDictionary(key=5, value=self.atom_b)
        right = HashDictionary(key=15, value=self.atom_c)
        root.previous = left
        root.next = right

        balanced = root._rebalance()
        self.assertEqual(balanced.key, 10, "Balanced tree should not change the root.")

        # Desbalance (rotación derecha)
        unbalanced = HashDictionary(key=10, previous=HashDictionary(key=5, previous=HashDictionary(key=2)))
        rebalanced = unbalanced._rebalance()
        self.assertEqual(rebalanced.key, 5, "Root should change after right rotation.")

        # Desbalance (rotación izquierda)
        unbalanced = HashDictionary(key=10, next=HashDictionary(key=15, next=HashDictionary(key=20)))
        rebalanced = unbalanced._rebalance()
        self.assertEqual(rebalanced.key, 15, "Root should change after left rotation.")

    # --- Test remove_key ---
    def test_remove_at(self):
        content = self.empty_dictionary
        content = content.set_at(10, self.atom_a)
        content = content.set_at(20, self.atom_b)
        content = content.set_at(15, self.atom_c)

        content = content.remove_at(20)
        self.assertEqual(content.count, 2, "Should has a count of 2 at this point")
        self.assertIsNone(content.get_at(20), "Should remove the key 20 from the content.")

    # --- Test as_iterable ---
    def test_as_iterable(self):
        content = self.empty_dictionary.set_at(10, self.atom_a)
        content = content.set_at(20, self.atom_b)
        content = content.set_at(15, self.atom_c)

        result = [r for r in content.as_iterable()]
        self.assertEqual(len(result), 3, "Should include all inserted keys in the iterable.")
        self.assertIn((10, self.atom_a), result, "Should include the correct key-value pair.")

    # --- Test merge method ---
    def test_merge(self):
        """Test merging two HashDictionary instances."""
        # Create two dictionaries with different keys
        dict1 = self.empty_dictionary.set_at(10, self.atom_a).set_at(20, self.atom_b)
        dict2 = self.empty_dictionary.set_at(30, self.atom_c).set_at(40, Atom())

        # Merge dict2 into dict1
        merged_dict = dict1.merge(dict2)

        # Verify all keys from both dictionaries are present
        self.assertEqual(merged_dict.count, 4, "Merged dictionary should have all keys from both dictionaries.")
        self.assertEqual(merged_dict.get_at(10), self.atom_a, "Key from dict1 should be preserved.")
        self.assertEqual(merged_dict.get_at(20), self.atom_b, "Key from dict1 should be preserved.")
        self.assertEqual(merged_dict.get_at(30), self.atom_c, "Key from dict2 should be added.")
        self.assertIsNotNone(merged_dict.get_at(40), "Key from dict2 should be added.")

        # Test merging with overlapping keys (the value from the second dictionary should take precedence)
        dict3 = self.empty_dictionary.set_at(10, Atom(value="new_value"))
        merged_with_overlap = dict1.merge(dict3)
        self.assertEqual(merged_with_overlap.get_at(10).value, "new_value", 
                         "Value from the second dictionary should override the first.")

    # --- Test _get_first method ---
    def test_get_first(self):
        """Test getting the first (smallest) key-value pair."""
        # Create a dictionary with multiple keys
        test_dict = (self.empty_dictionary
                    .set_at(30, self.atom_a)
                    .set_at(10, self.atom_b)
                    .set_at(20, self.atom_c))

        # Get the first (smallest) key-value pair
        first_key, first_value = test_dict._get_first()

        # Verify the smallest key is returned
        self.assertEqual(first_key, 10, "The smallest key should be returned.")
        self.assertEqual(first_value, self.atom_b, "The value associated with the smallest key should be returned.")

        # Test with an empty dictionary
        empty_result = self.empty_dictionary._get_first()
        self.assertIsNone(empty_result, "Empty dictionary should return None.")

    # --- Test _get_last method ---
    def test_get_last(self):
        """Test getting the last (largest) key-value pair."""
        # Create a dictionary with multiple keys
        test_dict = (self.empty_dictionary
                    .set_at(30, self.atom_a)
                    .set_at(10, self.atom_b)
                    .set_at(20, self.atom_c))

        # Get the last (largest) key-value pair
        last_key, last_value = test_dict._get_last()

        # Verify the largest key is returned
        self.assertEqual(last_key, 30, "The largest key should be returned.")
        self.assertEqual(last_value, self.atom_a, "The value associated with the largest key should be returned.")

        # Test with an empty dictionary
        empty_result = self.empty_dictionary._get_last()
        self.assertIsNone(empty_result, "Empty dictionary should return None.")

    # --- Test has method ---
    def test_has(self):
        """Test checking if a key exists in the dictionary."""
        # Create a dictionary with some keys
        test_dict = self.empty_dictionary.set_at(10, self.atom_a).set_at(20, self.atom_b)

        # Check for existing keys
        self.assertTrue(test_dict.has(10), "Should return True for an existing key.")
        self.assertTrue(test_dict.has(20), "Should return True for an existing key.")

        # Check for non-existing keys
        self.assertFalse(test_dict.has(30), "Should return False for a non-existing key.")
        self.assertFalse(self.empty_dictionary.has(10), "Empty dictionary should return False for any key.")

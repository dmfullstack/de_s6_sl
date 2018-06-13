package com.stackroute.datamunger.query;

//this class contains methods to evaluate expressions
public class Filter {
	/*
	 * the evaluateExpression() method of this class is responsible for evaluating
	 * the expressions mentioned in the query. It has to be noted that the process
	 * of evaluating expressions will be different for different data types. there
	 * are 6 operators that can exist within a query i.e. >=,<=,<,>,!=,= This method
	 * should be able to evaluate all of them.
	 * Note: while evaluating string expressions, please handle uppercase and lowercase
	 * 
	 */
	public boolean evaluateExpression(String operator, String firstInput, String secondInput, String dataType) {
		switch (operator) {
		case "=":
			if (equalTo(firstInput, secondInput, dataType))
				return true;
			else
				return false;
		case "!=":
			if (!equalTo(firstInput, secondInput, dataType))
				return true;
			else
				return false;
		case ">=":
			if (greaterThanOrEqualTo(firstInput, secondInput, dataType))
				return true;
			else
				return false;
		case "<=":
			if (lessThanOrEqualTo(firstInput, secondInput, dataType))
				return true;
			else
				return false;
		case ">":
			if (greaterThan(firstInput, secondInput, dataType))
				return true;
			else
				return false;
		case "<":
			if (lessThan(firstInput, secondInput, dataType))
				return true;
			else
				return false;
		}
		return false;
	}

	public double sum(double firstInput, String secondInput, String dataType) {
		switch (dataType) {
		case "java.lang.Integer":
			try {
				return (firstInput + Integer.parseInt(secondInput));
			} catch (Exception e) {
				return (firstInput + 0);
			}
		case "java.lang.Double":
			try {
				return firstInput + Double.parseDouble(secondInput);
			} catch (Exception e) {
				return (firstInput + 0);
			}
		default:
			throw new NumberFormatException("the selected column is a String or date");
		}
	}

	public boolean equalTo(String firstInput, String secondInput, String dataType) {
		switch (dataType) {
		case "java.lang.Integer":

		case "java.lang.Double":

		default:
			try {
				if (firstInput.equalsIgnoreCase(secondInput))
					return true;
				else
					return false;
			} catch (Exception e) {
				return false;
			}
		}
	}

	public boolean greaterThan(String firstInput, String secondInput, String dataType) {
		switch (dataType) {
		case "java.lang.Integer":

		case "java.lang.Double":
			try {
				if (Double.parseDouble(firstInput) > Double.parseDouble(secondInput))
					return true;
				else
					return false;
			} catch (Exception nfe) {
				return false;
			}
		default:
			try {
				if ((firstInput.compareToIgnoreCase(secondInput)) > 0)
					return true;
				else
					return false;
			} catch (Exception e) {
				return false;
			}
		}
	}

	public boolean greaterThanOrEqualTo(String firstInput, String secondInput, String dataType) {
		switch (dataType) {
		case "java.lang.Integer":

		case "java.lang.Double":
			try {
				if (Double.parseDouble(firstInput) >= Double.parseDouble(secondInput))
					return true;
				else
					return false;
			} catch (Exception nfe) {
				return false;
			}
		default:
			try {
				if ((firstInput.compareToIgnoreCase(secondInput)) >= 0)
					return true;
				else
					return false;
			} catch (Exception e) {
				return false;
			}
		}
	}

	public boolean lessThan(String firstInput, String secondInput, String dataType) {
		switch (dataType) {
		case "java.lang.Integer":

		case "java.lang.Double":
			try {
				if (Double.parseDouble(firstInput) < Double.parseDouble(secondInput))
					return true;
				else
					return false;
			} catch (Exception nfe) {
				return false;
			}
		default:
			try {
				if ((firstInput.compareToIgnoreCase(secondInput)) < 0)
					return true;
				else
					return false;
			} catch (Exception e) {
				return false;
			}
		}
	}

	public boolean lessThanOrEqualTo(String firstInput, String secondInput, String dataType) {
		switch (dataType) {
		case "java.lang.Integer":

		case "java.lang.Double":
			try {
				if (Double.parseDouble(firstInput) <= Double.parseDouble(secondInput))
					return true;
				else
					return false;
			} catch (Exception nfe) {
				return false;
			}
		default:
			try {
				if ((firstInput.compareToIgnoreCase(secondInput)) <= 0)
					return true;
				else
					return false;
			} catch (Exception e) {
				return false;
			}
		}
	}
}

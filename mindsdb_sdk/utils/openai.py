
import inspect
import docstring_parser


def make_openai_tool(function: callable):
    """
    Make an OpenAI tool for a function

    :param function: function to generate metadata for
    :return: dictionary containing function metadata
    """
    params = inspect.signature(function).parameters
    docstring = docstring_parser.parse(function.__doc__)

    function_dict = {
        "type":"function",
        "function":{
            "name":function.__name__,
            "description":docstring.short_description,
            "parameters":{
                "type":"object",
                "properties":{},
                "required":[]
            }
        }
    }

    for name, param in params.items():
        param_description = next((p.description for p in docstring.params if p.arg_name == name), '')

        # convert annotation type to string
        if param.annotation is not inspect.Parameter.empty:
            if inspect.isclass(param.annotation):
                param_type = param.annotation.__name__
            else:
                param_type = str(param.annotation)
        else:
            param_type = None

        function_dict["function"]["parameters"]["properties"][name] = {
            "type":param_type,
            "description":param_description
        }

        # Check if parameter is required
        if param.default == inspect.Parameter.empty:
            function_dict["function"]["parameters"]["required"].append(name)

    return function_dict


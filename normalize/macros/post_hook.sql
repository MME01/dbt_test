{% macro apply_default_policy_tag(relation, policy_tag) %}
    {% if execute %}

        {% set model_columns = model.get('columns') %}
        {% set model_columns_with_default_policy = {} %}
        {% set relation_columns = adapter.get_columns_in_relation(relation) %}
                
        {% for column in relation_columns %}
            {% set column_name = column.name %}
            {% if column_name not in model_columns %}
                {% do log('Applied default policy tag to ' ~ relation ~ '.' ~ column_name, info=true) %}
                {% do model_columns_with_default_policy.update({
                    column_name: {
                        'name': column_name,
                        'description': '',
                        'meta': {},
                        'data_type': None,
                        'quote': None,
                        'tags': [],
                        'policy_tags': [ policy_tag ]
                    }
                }) %}
            {% else %}
                {% do model_columns_with_default_policy.update({ column_name: model_columns[column_name] }) %}
            {% endif %}
        {% endfor %}
        {% do adapter.update_columns(relation, model_columns_with_default_policy) %}
    {% endif %}
{% endmacro %}
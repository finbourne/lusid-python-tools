from IPython.core.display import display, HTML


def toggle_code(button_message):

    toggle_code_str = f"""
    <form action="javascript:code_toggle()"><input type="submit" id="toggleButton" value="{button_message}"></form>
    """

    toggle_code_prepare_str = """
         <script>
         function code_toggle() {
             if ($('div.cell.code_cell.rendered.selected div.input').css('display')!='none'){
                 $('div.cell.code_cell.rendered.selected div.input').hide();
             } else {
                 $('div.cell.code_cell.rendered.selected div.input').show();
             }
         }
         </script>

     """

    return display(HTML(toggle_code_str + toggle_code_prepare_str))

from IPython.core.display import display, HTML

toggle_code_str = """
<form action="javascript:code_toggle()"><input type="submit" id="toggleButton" value="Toggle Docstring"></form>
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


def toggle_code():
    return display(HTML(toggle_code_str + toggle_code_prepare_str))

/**
 * Módulo JavaScript para controle de tela cheia no dashboard.
 * Fornece funcionalidade para alternar entre modo normal e tela cheia.
 */

window.dash_clientside = Object.assign({}, window.dash_clientside, {
    telaCheia: {
        /**
         * Alterna entre modo tela cheia e modo normal.
         * Usa a API Fullscreen do navegador para controlar o estado.
         * 
         * @returns {undefined} Retorna undefined para não atualizar nenhum output no Dash
         */
        alternarTelaCheia: function() {
            if (!document.fullscreenElement) {
                document.documentElement.requestFullscreen().catch(err => {
                    console.error(`Erro ao tentar entrar em tela cheia: ${err.message} (${err.name})`);
                });
            } else {
                document.exitFullscreen();
            }
            return window.dash_clientside.no_update; // Não atualiza nenhum output no Dash
        }
    }
});